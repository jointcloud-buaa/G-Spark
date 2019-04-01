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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.{HashMap, HashSet}
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.util.Utils

private[spark] class SiteContext(
  config: SparkConf, val ioEncryptionKey: Option[Array[Byte]]
) extends Logging {

  private var _conf: SparkConf = _
  private var _env: SparkEnv = _
  private var _taskScheduler: SiteTaskScheduler = _
  private var _schedulerBackend: SiteSchedulerBackend = _
  private var _heartbeatReceiver: RpcEndpointRef = _

  private var _siteAppId: String = _

  // get from the SiteDriverWrapper's command argument
  private var _appId: String = _
  private var _siteDriverId: String = _
  private var _siteDriverCores: Int = _
  private var _hostname: String = _
  private var _gdriverUrl: String = _
  private var _siteMasterUrl: String = _

  private var _executorMemory: Int = _

  private[spark] val executorEnvs = HashMap.empty[String, String]
  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  val startTime: Long = System.currentTimeMillis()

  def isStopped: Boolean = stopped.get()

  private[spark] def conf: SparkConf = _conf

  def getConf: SparkConf = conf.clone

  private[spark] def env: SparkEnv = _env

  private[spark] def taskScheduler: SiteTaskScheduler = _taskScheduler

  private[spark] def schedulerBackend: SiteSchedulerBackend = _schedulerBackend

  private[spark] def executorMemory: Int = _executorMemory

  def siteAppId: String = _siteAppId
  def siteMasterUrl: String = _siteMasterUrl
  def siteDriverId: String = _siteDriverId
  def globalDriverUrl: String = _gdriverUrl
  def hostname: String = _hostname
  def cores: Int = _siteDriverCores

  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    value
  }

  // TODO-lzp: 什么样的配置是有效的?
  private[spark] def validateSettings(): Unit = {

  }

  try {
    _conf = config.clone

    _appId = _conf.get("spark.app.id")
    _siteDriverId = _conf.get("spark.siteDriver.id")
    _siteDriverCores = _conf.getInt("spark.siteDriver.cores", 2)
    _hostname = _conf.get("spark.siteDriver.host")
    _gdriverUrl = _conf.get("spark.siteDriver.gdriverUrl")
    _siteMasterUrl = _conf.get("spark.siteMaster.url")

    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo("Spark configuration:\n" + _conf.toDebugString)
    }

    _executorMemory = _conf.getOption("spark.executor.memory")
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM"))
        .map(warnSparkMem))
      .map(Utils.memoryStringToMb)
      .getOrElse(1024) // 默认1G

    for {
      (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))
    } {
      executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
      executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    // The Mesos scheduler backend relies on this environment variable to set executor memory.
    // TODO: Set this only in the Mesos scheduler.
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= _conf.getExecutorEnv

    _env = SparkEnv.createSiteDriverEnv(
      _conf, _siteDriverId, _hostname, _siteDriverCores, ioEncryptionKey, isLocal = false
    )

    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      ExecutorHeartbeatReceiver.ENDPOINT_NAME, new ExecutorHeartbeatReceiver(this)
    )

    val scheduler = new SiteTaskSchedulerImpl(this)
    val backend = new StandaloneSiteSchedulerBackend(scheduler, this, _siteMasterUrl)
    scheduler.initialize(backend)
    _taskScheduler = scheduler
    _schedulerBackend = backend
    _heartbeatReceiver.ask[Boolean](SiteTaskSchedulerIsSet)

    _taskScheduler.start()

    _siteAppId = _taskScheduler.siteAppId()
    _conf.set("spark.siteApp.id", _siteAppId)

    // TODO-lzp: 涉及到BlockManager的修改
    env.blockManager.initialize(_siteAppId)

//    env.metricsSystem.start()

    _taskScheduler.postStartHook() // 等待集群OK
    _schedulerBackend.reportClusterReady()

    // TODO-lzp: 一些关于SiteDriver的清理

  } catch {
    case NonFatal(e) =>
      logError("Error initializing SiteContext.", e)
      try {
        stop()
      } catch {
        case NonFatal(inner) =>
          logError("Error stopping SiteContext after init error.", inner)
      } finally {
        throw e
      }
  }

  // from executor heartbeat receiver
  private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSiteSchedulerBackend =>
        b.killExecutors(Seq(executorId), replace = true, force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is only supported in coarse-grained mode")
        false
    }
  }

  private[spark] def stopInNewThread(): Unit = {
    new Thread("stop-site-context") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          SiteContext.this.stop()
        } catch {
          case e: Throwable =>
            logError(e.getMessage, e)
            throw e
        }
      }
    }.start()
  }

  // TODO-lzp: 目前只是比较粗略
  def stop(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      logInfo("SiteContext already stopped")
      return
    }

    if (env != null && _heartbeatReceiver != null) {
      Utils.tryLogNonFatalError {
        env.rpcEnv.stop(_heartbeatReceiver)
      }
    }

    logInfo("Successfully stopped SiteContext")
  }
}
