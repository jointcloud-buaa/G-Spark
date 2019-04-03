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

package org.apache.spark

import java.io.File
import java.net.Socket

import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    // the global driver does not need a commit coordinator
    val outputCommitCoordinator: Option[OutputCommitCoordinator],
    val conf: SparkConf) extends Logging {

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private[spark] var driverTmpDir: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.foreach(_.stop())
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val globalDriverSystemName = "sparkGlobalDriver"
  private[spark] val siteDriverSystemName = "sparkSiteDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the global driver.
   */
  private[spark] def createGlobalDriverEnv(
    conf: SparkConf,
    isLocal: Boolean,
    listenerBus: LiveListenerBus,
    numCores: Int
    ): SparkEnv = {
    assert(conf.contains(GLOBAL_DRIVER_HOST_ADDRESS),
      s"${GLOBAL_DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains("spark.globalDriver.port"),
      "spark.globalDriver.port is not set on the driver!")
    val bindAddress = conf.get(GLOBAL_DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(GLOBAL_DRIVER_HOST_ADDRESS)
    val port = conf.get("spark.globalDriver.port").toInt
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    val execId = SparkContext.GLOBAL_DRIVER_IDENTIFIER  // global-driver

    // only global driver has listenerBus
    assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")

    val securityManager = new SecurityManager(conf, ioEncryptionKey)
    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isSaslEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val rpcEnv = RpcEnv.create(globalDriverSystemName, bindAddress, advertiseAddress, port, conf,
      securityManager, false)  // 如果是driver则startServer, 否则不执行startServer
    conf.set("spark.globalDriver.port", rpcEnv.address.port.toString)  // driver最终绑定的端口

    // 实例化序列器, 默认JavaSerializer类
    val serializerName = conf.get(
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val serializer = instantiateClass1[Serializer](serializerName, conf)
    logDebug(s"Using serializer: ${serializer.getClass}")
    // 1st参数是默认的序列化类
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
    val closureSerializer = new JavaSerializer(conf)

    // TODO-IMP: 此处的1st参数, 至少在当前实现中, 没有用
    val broadcastManager = new BroadcastManager(true, conf, securityManager)

    val mapOutputTracker = new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    logInfo("Registering " + MapOutputTracker.ENDPOINT_NAME)
    mapOutputTracker.trackerEndpoint = rpcEnv.setupEndpoint(
      MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf)
    )

    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass1[ShuffleManager](shuffleMgrClass, conf)

    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager = if (useLegacyMemoryManager) {
      new StaticMemoryManager(conf, numCores)
    } else {
      UnifiedMemoryManager(conf, numCores)
    }

    val blockManagerPort = conf.get(GLOBAL_DRIVER_BLOCK_MANAGER_PORT)
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numCores)
    logInfo(s"Registering ${BlockManagerMaster.GLOBAL_DRIVER_ENDPOINT_NAME}")
    val blockManagerGlobalMasterEndpointRef = rpcEnv.setupEndpoint(
      BlockManagerMaster.GLOBAL_DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(execId, rpcEnv, isLocal, conf, Some(listenerBus))
    )
    val blockManagerGlobalMaster = new BlockManagerMaster(
      execId, blockManagerGlobalMasterEndpointRef, conf
    )
    val blockManager = new BlockManager(execId, rpcEnv,
      blockManagerGlobalMaster, serializerManager, conf, memoryManager, mapOutputTracker,
      shuffleManager, blockTransferService, securityManager, numCores)

    // ==
    val metricsSystem = MetricsSystem.createMetricsSystem("driver", conf, securityManager)

    val envInstance = new SparkEnv(
      execId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      None,
      conf)

    val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    envInstance.driverTmpDir = Some(sparkFilesDir)
    envInstance
  }

  private[spark] def createSiteDriverEnv(
    conf: SparkConf,
    execId: String,  // siteDriverId
    hostname: String,
    // TODO-lzp: 如果SiteDriver是与应用无关的服务, 则可以为0
    numCores: Int,
    ioEncryptionKey: Option[Array[Byte]],
    isLocal: Boolean,
    mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    val port = conf.getInt("spark.siteDriver.port", 0)

    val securityManager = new SecurityManager(conf, ioEncryptionKey)
    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isSaslEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val rpcEnv = RpcEnv.create(siteDriverSystemName, hostname, hostname, port, conf,
      securityManager, false)
    conf.set("spark.siteDriver.port", rpcEnv.address.port.toString)

    // 实例化序列器, 默认JavaSerializer类
    val serializerName = conf.get(
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val serializer = instantiateClass1[Serializer](serializerName, conf)
    logDebug(s"Using serializer: ${serializer.getClass}")
    // 1st参数是默认的序列化类
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
    val closureSerializer = new JavaSerializer(conf)

    // TODO-IMP: 此处的1st参数, 至少在当前实现中, 没有用
    val broadcastManager = new BroadcastManager(true, conf, securityManager)

    // TODO-lzp: 这里至少应该设置MapOutputTrackerSiteMaster
    val mapOutputTracker = new MapOutputTrackerMaster(conf, broadcastManager, isLocal)

    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass1[ShuffleManager](shuffleMgrClass, conf)

    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    // MemoryManager的cores用来决定页大小
    val memoryManager: MemoryManager = if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numCores)
      } else {
        UnifiedMemoryManager(conf, numCores)
      }

    val blockManagerPort = conf.get(SITE_DRIVER_BLOCK_MANAGER_PORT)
    val blockTransferService = new NettyBlockTransferService(
      conf, securityManager, hostname, hostname, blockManagerPort, numCores)
    val blockManagerGlobalMasterRef = RpcUtils.makeRef(
      BlockManagerMaster.GLOBAL_DRIVER_ENDPOINT_NAME,
      conf.get("spark.globalDriver.host", "localhost"),
      conf.getInt("spark.globalDriver.port", 7077),
      rpcEnv
    )
    val blockManagerMasterRef = rpcEnv.setupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      // TODO-lzp: None, 这里要考虑是否要在SiteDriver上启动ListenerBus
      new BlockManagerMasterEndpoint(execId, rpcEnv, isLocal, conf, None)
    )
    val blockManagerMaster = new BlockManagerMaster(execId, blockManagerMasterRef, conf)
    blockManagerMaster.setGlobalDriverEndpoint(blockManagerGlobalMasterRef)
    val blockManager = new BlockManager(execId, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numCores)

    // TODO-lzp: 待进一步确定
    conf.set("spark.executor.id", execId)
    val metricsSystem = MetricsSystem.createMetricsSystem("siteDriver", conf, securityManager)
    metricsSystem.start()

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, true) }
    val occName = "OutputCommitCoordinator"
    logInfo(s"Registering $occName")
    val outputCommitCoordinatorRef = rpcEnv.setupEndpoint(
      occName, new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      execId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      Some(outputCommitCoordinator),
      conf)

    // TODO-lzp: 设置目录, 用于存放通过SparkContext#addFile()添加的文件, 不确定
    val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    envInstance.driverTmpDir = Some(sparkFilesDir)

    // TODO-lzp: 不确定是否在此处设置
    SparkEnv.set(envInstance)
    envInstance
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      execId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {

    val securityManager = new SecurityManager(conf, ioEncryptionKey)
    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isSaslEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val rpcEnv = RpcEnv.create(executorSystemName, hostname, hostname, port, conf,
      securityManager, true)
    if (rpcEnv.address != null) {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
      logInfo(s"Setting spark.executor.port to: ${rpcEnv.address.port.toString}")
    }

    // 实例化序列器, 默认JavaSerializer类
    val serializerName = conf.get(
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val serializer = instantiateClass1[Serializer](serializerName, conf)
    logDebug(s"Using serializer: ${serializer.getClass}")
    // 1st参数是默认的序列化类
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
    val closureSerializer = new JavaSerializer(conf)

    // 此处的1st参数, 至少在当前实现中, 没有用
    val broadcastManager = new BroadcastManager(false, conf, securityManager)

    val mapOutputTracker = new MapOutputTrackerWorker(conf)
//    mapOutputTracker.trackerEndpoint = RpcUtils.makeRef(
//      MapOutputTracker.ENDPOINT_NAME,
//      conf.get("spark.siteDriver.host", "localhost"),
//      conf.getInt("spark.siteDriver.port", 7077),
//      rpcEnv
//    )

    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass1[ShuffleManager](shuffleMgrClass, conf)

    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager = if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numCores)
      } else {
        UnifiedMemoryManager(conf, numCores)
      }

    val blockManagerPort = conf.get(BLOCK_MANAGER_PORT)
    val blockTransferService = new NettyBlockTransferService(
      conf, securityManager, hostname, hostname, blockManagerPort, numCores)
    val blockManagerMasterRef = null
//    val blockManagerMasterRef = RpcUtils.makeRef(
//      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
//      conf.get("spark.siteDriver.host", "localhost"),
//      conf.getInt("spark.siteDriver.port", 7077),
//      rpcEnv
//    )
    // TODO-lzp: about the isDriver
    val blockManagerMaster = new BlockManagerMaster(execId, blockManagerMasterRef, conf)
    val blockManager = new BlockManager(execId, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numCores)

    conf.set("spark.executor.id", execId)
    val metricsSystem = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
    metricsSystem.start()

    val outputCommitCoordinator = new OutputCommitCoordinator(conf, false)
    val outputCommitCoordinatorRef = RpcUtils.makeRef(
      "OutputCommitCoordinator",
      conf.get("spark.siteDriver.host", "localhost"),
      conf.getInt("spark.siteDriver.port", 7077),
      rpcEnv
    )
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      execId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      Some(outputCommitCoordinator),
      conf)

    SparkEnv.set(envInstance)
    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }

  // 实例化一个以conf为唯一构造参数的类
  def instantiateClass1[T](className: String, conf: SparkConf): T = Utils.classForName(className)
    .getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
}
