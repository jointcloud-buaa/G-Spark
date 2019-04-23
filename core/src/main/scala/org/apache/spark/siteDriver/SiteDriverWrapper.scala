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

import scala.collection.mutable.ListBuffer

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.sitemaster.SiteMasterWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.util.Utils

object SiteDriverWrapper extends Logging {
  private def run(
    gdriverUrl: String,
    siteDriverId: String,
    hostname: String, // TODO-lzp: do what?
    cores: Int,
    appId: String,
    siteMasterUrl: String,
    clusterName: String,
    userClassPath: Seq[URL]
  ): Unit = {

    Utils.initDaemon(log)

    // TODO-lzp: 不是很懂这里的强调的hadoop用户是什么
    SparkHadoopUtil.get.runAsSparkUser { () =>
      // 1, 从global driver处接受配置信息
      val sdriverConf = new SparkConf
      val port = sdriverConf.getInt("spark.siteDriver.port", 0)
      val rpcEnv = RpcEnv.create("SiteDriver", hostname, port, sdriverConf,
        new SecurityManager(sdriverConf)
      )
      rpcEnv.setupEndpoint("siteMasterWatcher", new SiteMasterWatcher(rpcEnv, siteMasterUrl))
      val gdriver = rpcEnv.setupEndpointRefByURI(gdriverUrl)
      val cfg = gdriver.askWithRetry[SparkAppConfig](RetrieveSparkAppConfig)
      val props = cfg.sparkProperties ++ Seq[(String, String)](
        ("spark.app.id", appId),
        ("spark.siteDriver.id", siteDriverId),
        ("spark.siteDriver.cores", cores.toString),
        ("spark.siteDriver.host", hostname),
        ("spark.siteDriver.gdriverUrl", gdriverUrl),
        ("spark.siteMaster.url", siteMasterUrl),
        ("spark.siteMaster.name", clusterName)
      )
      // Create SparkEnv using properties we fetched from the driver.
      val gdriverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isComponetStartupConf(key)) {
          // 简单说, 不会覆盖本地的关于启动的配置
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

      val ssc = new SiteContext(
        gdriverConf, cfg.ioEncryptionKey, userClassPath
      )


      // TODO-lzp: do what??
      // 8, 需不需要搞个listenerBus, 还是把类似消息直接传递给gd, 再传递给gd's listenerbus

      // TODO-lzp: 不确定能否阻塞至应用结束
      rpcEnv.awaitTermination()
      SparkHadoopUtil.get.stopCredentialUpdater()
    }
  }

  def main(args: Array[String]): Unit = {
    var gdriverUrl: String = null
    var siteDriverId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var siteMasterUrl: String = null
    var clusterName: String = null
    val userClassPath = new ListBuffer[URL]()

    var argv = args.toList
    while (argv.nonEmpty) {
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
        case "--cluster-name" :: value :: tail =>
          clusterName = value
          argv = tail
        case "--site-master-url" :: value :: tail =>
          siteMasterUrl = value
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
    if (gdriverUrl == null || siteDriverId == null || hostname == null || appId == null) {
      printUsageAndExit()
    }

    run(gdriverUrl, siteDriverId, hostname, cores, appId, siteMasterUrl, clusterName, userClassPath)
    System.exit(0)
  }

  private def printUsageAndExit(): Unit = {
    // scalastyle:off println
    System.err.println(
      """
        |Usage: SiteDriverWrapper [options]
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
