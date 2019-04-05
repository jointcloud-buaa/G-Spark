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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import org.apache.spark.{ComponentContext, ContextCleaner, SparkConf, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.util.Utils

private[spark] class SiteContext(
  config: SparkConf, val ioEncryptionKey: Option[Array[Byte]]
) extends ComponentContext with Logging {

  private var _conf: SparkConf = _
  private var _env: SparkEnv = _
  private var _taskScheduler: TaskScheduler = _
  private var _schedulerBackend: SiteSchedulerBackend = _
  @volatile private var _stageScheduler: StageScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  private var _cleaner: Option[ContextCleaner] = None

  private var _siteAppId: String = _
  private var _siteAppAttemptId: Option[String] = _

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

  def isLocal: Boolean = Utils.isLocalMaster(_conf)

  def isStopped: Boolean = stopped.get()

  private[spark] val listenerBus = new LiveListenerBus(this)

  override def conf: SparkConf = _conf

  def getConf: SparkConf = conf.clone

  private[spark] def env: SparkEnv = _env

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler

  private[spark] def schedulerBackend: SiteSchedulerBackend = _schedulerBackend

  private[spark] def cleaner: Option[ContextCleaner] = _cleaner

  // 为_dagScheduler任务调度器设置getter/setter方法
  private[spark] def stageScheduler: StageScheduler = _stageScheduler

  private[spark] def stageScheduler_=(ds: StageScheduler): Unit = {
    _stageScheduler = ds
  }

  private[spark] def executorMemory: Int = _executorMemory

  def siteAppId: String = _siteAppId
  def siteAppAttemptId: Option[String] = _siteAppAttemptId
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

    val scheduler = new TaskSchedulerImpl(this)
    val backend = new StandaloneSchedulerBackend(scheduler, this, _siteMasterUrl)
    scheduler.initialize(backend)
    _taskScheduler = scheduler
    _schedulerBackend = backend
    _stageScheduler = new StageScheduler(this)
    _heartbeatReceiver.ask[Boolean](SiteTaskSchedulerIsSet)

    _taskScheduler.start()

    _siteAppId = _taskScheduler.siteAppId()
    _siteAppAttemptId = taskScheduler.siteAppAttemptId()

    _conf.set("spark.siteApp.id", _siteAppId)

    // TODO-lzp: 涉及到BlockManager的修改
    env.blockManager.initialize(_siteAppId)

    // TODO-lzp: 传递给ContextCleaner的Context还略微有些麻烦
//    _cleaner =
//      if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
//        Some(new ContextCleaner(this))
//      } else {
//        None
//      }
    _cleaner = None
    _cleaner.foreach(_.start())

    //    env.metricsSystem.start()

    _taskScheduler.postStartHook() // 等待集群OK
    _env.metricsSystem.registerSource(_stageScheduler.metricsSource)
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

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
   * 广播一个只读变量到集群, 并返回一个Broadcast对象, 用来在分布式函数中读取.
   * 变量将发送到每个集群, 只一次
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    assertNotStopped()
    // 不能直接广播RDD, 只能调用collect再广播它的结果
    require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
      "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.")
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
    logInfo("Created broadcast " + bc.id)
    cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  // TODO-lzp: 设立ListenerBus
  private def setupAndStartListenerBus(): Unit = {

  }

  // 检测SparkContext未停止, 如果停止则抛出异常
  private[spark] def assertNotStopped(): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("Cannot call methods on a stopped SparkContext.")
    }
  }

  // from executor heartbeat receiver
  private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutors(Seq(executorId), replace = true, force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is only supported in coarse-grained mode")
        false
    }
  }

  override def stopInNewThread(): Unit = {
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

  // TODO-lzp: 目前只是比较粗
  override def stop(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      logInfo("SiteContext already stopped")
      return
    }

    if (env != null && _heartbeatReceiver != null) {
      Utils.tryLogNonFatalError {
        env.rpcEnv.stop(_heartbeatReceiver)
      }
    }
    Utils.tryLogNonFatalError {
      _cleaner.foreach(_.stop())
    }

    if (_stageScheduler != null) {
      Utils.tryLogNonFatalError {
        _stageScheduler.stop()
      }
      _stageScheduler = null
    }

    logInfo("Successfully stopped SiteContext")
  }
}

object SiteContext extends Logging {
}
