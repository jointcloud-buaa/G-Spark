/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.siteDriver

import java.net.URL
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.sitemaster.SiteMasterWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.SiteDriverLossReason
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterSiteDriver, RemoveSiteDriver, RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, Utils}

class CoarseGrainedSiteDriverBackend(
  override val rpcEnv: RpcEnv,
  gdriverUrl: String,
  siteDriverId: String,
  hostname: String,
  cores: Int,
  userClassPath: Seq[URL],
  env: SparkEnv
) extends ThreadSafeRpcEndpoint with Logging {

  private[this] val stopping = new AtomicBoolean(false)
  @volatile var gdriver: Option[RpcEndpointRef] = None
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def onStart() {
    logInfo("Connecting to global driver: " + gdriverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(gdriverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      gdriver = Some(ref)  // 赋予了driver的Ref
      // 向Driver注册Executor
      ref.ask[Boolean](RegisterSiteDriver(siteDriverId, self, hostname, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
      // Always receive `true`. Just ignore it
      case Failure(e) =>
        exitSiteDriver(1, s"Cannot register with driver: $gdriverUrl", e,
          notifyGlobalDriver = false)
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
  }

  protected def exitSiteDriver(code: Int,
    reason: String,
    throwable: Throwable = null,
    notifyGlobalDriver: Boolean = true) = {
    val message = "SiteDriver self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }

    if (notifyGlobalDriver && gdriver.nonEmpty) {
      gdriver.get.ask[Boolean](
        RemoveSiteDriver(siteDriverId, new SiteDriverLossReason(reason))
      ).onFailure { case e =>
        logWarning(s"Unable to notify the driver due to " + e.getMessage, e)
      }(ThreadUtils.sameThread)
    }

    System.exit(code)
  }

}

object CoarseGrainedSiteDriverBackend extends Logging {

  private def run(
    gdriverUrl: String,
    siteDriverId: String,
    hostname: String,
    cores: Int,
    appId: String,
    siteMasterUrl: Option[String],
    userClassPath: Seq[URL]
  ): Unit = {
    Utils.initDaemon(log)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      Utils.checkHost(hostname)

      // TODO: start a RpcEnv just for fetching configration from Driver, It does not make sense
      val siteDriverConf = new SparkConf
      val port = siteDriverConf.getInt("spark.siteDriver.port", 0)
      val fetcher = RpcEnv.create(
        "siteDriverPropsFetcher",
        hostname,
        port,
        siteDriverConf,
        new SecurityManager(siteDriverConf),
        clientMode = true
      )
      val gdriver = fetcher.setupEndpointRefByURI(gdriverUrl)
      val cfg = gdriver.askWithRetry[SparkAppConfig](RetrieveSparkAppConfig)
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      val gdriverConf = new SparkConf
      for ((key, value) <- props) {
        if (SparkConf.isSiteDriverStartupConf(key)) {
          gdriverConf.setIfMissing(key, value)
        } else {
          gdriverConf.set(key, value)
        }
      }
      if (gdriverConf.contains("spark.yarn.credentials.file")) {
        logInfo("Will periodically update credentials from: " +
          gdriverConf.get("spark.yarn.credentials.file"))
        SparkHadoopUtil.get.startCredentialUpdater(gdriverConf)
      }

      val env = SparkEnv.createSiteDriverEnv(
        gdriverConf, siteDriverId, hostname, port, cores, cfg.ioEncryptionKey, isLocal = false
      )
      env.rpcEnv.setupEndpoint("SiteDriver", new CoarseGrainedSiteDriverBackend(
        env.rpcEnv, gdriverUrl, siteDriverId, hostname, cores, userClassPath, env
      ))
      siteMasterUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("SiteMasterWatcher", new SiteMasterWatcher(env.rpcEnv, url))
      }

      env.rpcEnv.awaitTermination()

      SparkHadoopUtil.get.stopCredentialUpdater()
    }
  }

  def main(args: Array[String]): Unit = {
    var gdriverUrl: String = null
    var siteDriverId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var siteMasterUrl: Option[String] = None
    val userClassPath = new ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case "--global-driver-url" :: value :: tail =>
          gdriverUrl = value
          argv = tail
        case "--site-driver-id" :: value :: tail =>
          siteDriverId = value
          argv = tail
        case "--hostname" :: value :: tail =>
          hostname = value
          argv = tail
        case "--cores" :: value :: tail =>
          cores = value.toInt
          argv = tail
        case "--app-id" :: value :: tail =>
          appId = value
          argv = tail
        case "--site-master-url" :: value :: tail =>
          siteMasterUrl = Some(value)
          argv = tail
        case "--user-class-path" :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }
    if (gdriverUrl == null || siteDriverId == null || hostname == null ||
      cores <= 0 || appId == null) {
      printUsageAndExit()
    }

    run(gdriverUrl, siteDriverId, hostname, cores, appId, siteMasterUrl, userClassPath)
    System.exit(0)
  }

  private def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
      """
        |Usage: CoarseGrainedExecutorBackend [options]
        |
        | Options are:
        |   --global-driver-url <driverUrl>
        |   --site-driver-id <executorId>
        |   --hostname <hostname>
        |   --cores <cores>
        |   --app-id <appid>
        |   --site-master-url <workerUrl>
        |   --user-class-path <url>
        |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}
