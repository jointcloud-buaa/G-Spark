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

import java.io.{File, FileNotFoundException}
import java.lang.reflect.Constructor
import java.net.{URI, URL}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{ComponentContext, ContextCleaner, MapOutputTrackerMaster, SparkConf, SparkEnv, SparkException, SparkFiles}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcAddress, RpcEndpointAddress, RpcEndpointRef}
import org.apache.spark.scheduler.{EventLoggingListener, LiveListenerBus, SparkListenerInterface}
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator, DoubleAccumulator, LongAccumulator, Utils}

private[spark] class SiteContext(
  config: SparkConf, val ioEncryptionKey: Option[Array[Byte]], _userClassPath: Seq[URL]
) extends ComponentContext with Logging {

  private var _conf: SparkConf = _
  private var _env: SparkEnv = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _hadoopConfiguration: Configuration = _
  private var _taskScheduler: TaskScheduler = _
  private var _schedulerBackend: SiteSchedulerBackend = _
  @volatile private var _stageScheduler: StageScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false

  private var _siteAppId: String = _
  private var _siteAppAttemptId: Option[String] = _

  // get from the SiteDriverWrapper's command argument
  private var _appId: String = _
  private var _siteDriverId: String = _
  private var _siteDriverCores: Int = _
  private var _hostname: String = _
  private var _gdriverUrl: String = _
  private var _siteMasterUrl: String = _
  private var _siteMasterName: String = _

  private var _executorMemory: Int = _

  // Used to store a URL for each static file/jar together with the file's local timestamp
  private[spark] val addedFiles = new ConcurrentHashMap[String, Long]().asScala
  private[spark] val addedJars = new ConcurrentHashMap[String, Long]().asScala

  private[spark] val executorEnvs = HashMap.empty[String, String]
  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  val startTime: Long = System.currentTimeMillis()

  def isLocal: Boolean = Utils.isLocalMaster(_conf)

  def isStopped: Boolean = stopped.get()

  private[spark] val listenerBus = new LiveListenerBus(this)

  override def conf: SparkConf = _conf

  def getConf: SparkConf = conf.clone

  override def env: SparkEnv = _env

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler

  private[spark] def schedulerBackend: SiteSchedulerBackend = _schedulerBackend

  override def cleaner: Option[ContextCleaner] = _cleaner

  // 为_dagScheduler任务调度器设置getter/setter方法
  private[spark] def stageScheduler: StageScheduler = _stageScheduler

  private[spark] def stageScheduler_=(ds: StageScheduler): Unit = {
    _stageScheduler = ds
  }

  def userClassPath(): Seq[URL] = _userClassPath

  /**
   * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
   *
   * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
   */
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  private[spark] def executorMemory: Int = _executorMemory

  def siteAppId: String = _siteAppId
  def siteAppAttemptId: Option[String] = _siteAppAttemptId
  def siteMasterUrl: String = _siteMasterUrl
  def clusterName: String = _siteMasterName
  def siteMasterHost: String = RpcEndpointAddress(_siteMasterUrl).rpcAddress.host
  def siteDriverId: String = _siteDriverId
  def globalDriverUrl: String = _gdriverUrl
  def hostname: String = _hostname
  def cores: Int = _siteDriverCores

  private[spark] def isEventLogEnabled: Boolean = _conf.getBoolean("spark.eventLog.enabled", false)
  // TODO-lzp: 以下用在SiteAppDescription的构造上
  private[spark] def eventLogDir: Option[URI] = _eventLogDir
  private[spark] def eventLogCodec: Option[String] = _eventLogCodec
  private[spark] def eventLogger: Option[EventLoggingListener] = _eventLogger

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
    _siteMasterName = _conf.get("spark.siteMaster.name")

    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo("Spark configuration:\n" + _conf.toDebugString)
    }

    // 其实可以不用是hdfs路径的
    _eventLogDir =
      if (isEventLogEnabled) {
        val unresolvedDir = conf.get(
          "spark.siteDriver.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR
        ).stripSuffix("/")
        Some(Utils.resolveURI(unresolvedDir))
      } else {
        None
      }

    _eventLogCodec = {
      val compress = _conf.getBoolean("spark.eventLog.compress", false)
      if (compress && isEventLogEnabled) {
        Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
      } else {
        None
      }
    }

    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)

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
      _conf, _siteDriverId, _hostname, listenerBus, _siteDriverCores, ioEncryptionKey,
      isLocal = false
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


    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_siteAppId, _siteAppAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        logger.start()
        listenerBus.addListener(logger)
        Some(logger)
      } else {
        None
      }

    env.blockManager.initialize(_siteAppId)

    // 让mapOutputTracker知晓SiteDriver's blockManagerId
    env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      .setBlockManagerId(env.blockManager.blockManagerId)

    _cleaner =
      if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
        Some(new ContextCleaner(this))
      } else {
        None
      }
    _cleaner.foreach(_.start())

    //    env.metricsSystem.start()
    setupAndStartListenerBus()

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
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   */
  def addJar(path: String) {
    if (path == null) {
      logWarning("null specified as parameter to addJar")
    } else {
      var key = ""
      if (path.contains("\\")) {
        // For local paths with backslashes on Windows, URI throws an exception
        key = env.rpcEnv.fileServer.addJar(new File(path))
      } else {
        val uri = new URI(path)
        // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
        Utils.validateURL(uri)
        key = uri.getScheme match {
          // A JAR file which exists only on the driver node
          case null | "file" =>
            try {
              val file = new File(uri.getPath)
              if (!file.exists()) {
                throw new FileNotFoundException(s"Jar ${file.getAbsolutePath} not found")
              }
              if (file.isDirectory) {
                throw new IllegalArgumentException(
                  s"Directory ${file.getAbsoluteFile} is not allowed for addJar")
              }
              env.rpcEnv.fileServer.addJar(new File(uri.getPath))
            } catch {
              case NonFatal(e) =>
                logError(s"Failed to add $path to Spark environment", e)
                null
            }
          // A JAR file which exists locally on every worker node
          case "local" =>
            "file:" + uri.getPath
          case _ =>
            path
        }
      }
      if (key != null) {
        val timestamp = System.currentTimeMillis
        if (addedJars.putIfAbsent(key, timestamp).isEmpty) {
          logInfo(s"Added JAR $path at $key with timestamp $timestamp")
//          postEnvironmentUpdate()
        }
      }
    }
  }

  /**
   * Returns a list of jar files that are added to resources.
   */
  def listJars(): Seq[String] = addedJars.keySet.toSeq

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   */
  def addFile(path: String): Unit = {
    addFile(path, false)
  }

  /**
   * Returns a list of file paths that are added to resources.
   */
  def listFiles(): Seq[String] = addedFiles.keySet.toSeq

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   *
   * A directory can be given if the recursive option is set to true. Currently directories are only
   * supported for Hadoop-supported filesystems.
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    val uri = new Path(path).toUri
    val schemeCorrectedPath = uri.getScheme match {
      case null | "local" => new File(path).getCanonicalFile.toURI.toString
      case _ => path
    }

    val hadoopPath = new Path(schemeCorrectedPath)
    val scheme = new URI(schemeCorrectedPath).getScheme
    if (!Array("http", "https", "ftp").contains(scheme)) {
      val fs = hadoopPath.getFileSystem(hadoopConfiguration)
      val isDir = fs.getFileStatus(hadoopPath).isDirectory
      if (!isLocal && scheme == "file" && isDir) {
        throw new SparkException(s"addFile does not support local directories when not running " +
          "local mode.")
      }
      if (!recursive && isDir) {
        throw new SparkException(s"Added file $hadoopPath is a directory and recursive is not " +
          "turned on.")
      }
    } else {
      // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
      Utils.validateURL(uri)
    }

    val key = if (!isLocal && scheme == "file") {
      env.rpcEnv.fileServer.addFile(new File(uri.getPath))
    } else {
      schemeCorrectedPath
    }
    val timestamp = System.currentTimeMillis
    if (addedFiles.putIfAbsent(key, timestamp).isEmpty) {
      logInfo(s"Added file $path at $key with timestamp $timestamp")
      // Fetch the file locally so that closures which are run on the driver can still use the
      // SparkFiles API to access files.
      // TODO-lzp: 这里HDFS的文件反而不是全局了, 如果添加file, 则file最好在当前集群的HDFS中存在.
      Utils.fetchFile(uri.toString, new File(SparkFiles.getRootDirectory()), conf,
        env.securityManager, hadoopConfiguration, timestamp, useCache = false)
//      postEnvironmentUpdate()
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

  /**
   * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def longAccumulator: LongAccumulator = {
    val acc = new LongAccumulator
    register(acc)
    acc
  }

  /**
   * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def longAccumulator(name: String): LongAccumulator = {
    val acc = new LongAccumulator
    register(acc, name)
    acc
  }

  /**
   * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def doubleAccumulator: DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc)
    acc
  }

  /**
   * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def doubleAccumulator(name: String): DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc, name)
    acc
  }

  /**
   * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
   * inputs by adding them into the list.
   */
  def collectionAccumulator[T]: CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc)
    acc
  }

  /**
   * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
   * inputs by adding them into the list.
   */
  def collectionAccumulator[T](name: String): CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc, name)
    acc
  }

  /**
   * :: DeveloperApi ::
   * Register a listener to receive up-calls from events that happen during execution.
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListenerInterface) {
    listenerBus.addListener(listener)
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

  private def setupAndStartListenerBus(): Unit = {
    // Use reflection to instantiate listeners specified via `spark.extraListeners`
    try {
      val listenerClassNames: Seq[String] =
        conf.get("spark.extraListeners", "").split(',').map(_.trim).filter(_ != "")
      for (className <- listenerClassNames) {
        // Use reflection to find the right constructor
        val constructors = {
          val listenerClass = Utils.classForName(className)
          listenerClass
            .getConstructors
            .asInstanceOf[Array[Constructor[_ <: SparkListenerInterface]]]
        }
        val constructorTakingSparkConf = constructors.find { c =>
          c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
        }
        lazy val zeroArgumentConstructor = constructors.find { c =>
          c.getParameterTypes.isEmpty
        }
        val listener: SparkListenerInterface = {
          if (constructorTakingSparkConf.isDefined) {
            constructorTakingSparkConf.get.newInstance(conf)
          } else if (zeroArgumentConstructor.isDefined) {
            zeroArgumentConstructor.get.newInstance()
          } else {
            throw new SparkException(
              s"$className did not have a zero-argument constructor or a" +
                " single-argument constructor that accepts SparkConf. Note: if the class is" +
                " defined inside of another Scala class, then its constructors may accept an" +
                " implicit parameter that references the enclosing class; in this case, you must" +
                " define the listener as a top-level class in order to prevent this extra" +
                " parameter from breaking Spark's ability to find a valid constructor.")
          }
        }
        listenerBus.addListener(listener)
        logInfo(s"Registered listener $className")
      }
    } catch {
      case e: Exception =>
        try {
          stop()
        } finally {
          throw new SparkException(s"Exception when registering SparkListener", e)
        }
    }

    listenerBus.start()
    _listenerBusStarted = true
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
    if (_listenerBusStarted) {
      Utils.tryLogNonFatalError {
        listenerBus.stop()
        _listenerBusStarted = false
      }
    }
    Utils.tryLogNonFatalError {
      _eventLogger.foreach(_.stop())
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
