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

package org.apache.spark.deploy.sitemaster

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, LinkedHashMap}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success}
import scala.util.control.NonFatal

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExecutorState
import org.apache.spark.deploy.globalmaster.{GlobalDriverState, GlobalMaster, SiteDriverState}
import org.apache.spark.deploy.leaderandrecovery._
import org.apache.spark.deploy.leaderandrecovery.LeaderMessages.{ElectedLeader, RevokedLeadership}
import org.apache.spark.deploy.leaderandrecovery.RecoveryMessages.CompleteRecovery
import org.apache.spark.deploy.sitemaster.SiteAppState.SiteAppState
import org.apache.spark.deploy.sitemaster.SiteMasterMessages.{BoundPortsRequest, BoundPortsResponse, CheckForWorkerTimeOut}
import org.apache.spark.deploy.sitemaster.ui.SiteMasterWebUI
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{ThreadUtils, Utils}

private[deploy] class SiteMaster(
  override val rpcEnv: RpcEnv,
  webUiPort: Int,
  cores: Int,
  memory: Int,
  gmRpcAddresses: Array[RpcAddress],
  endpointName: String,
  workDirPath: String = null,
  val conf: SparkConf,
  val securityMgr: SecurityManager
) extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {
  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("site-master-message-scheduler")

  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("site-master-cleanup-thread")
  )

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private val HEARTBEAT_MILLIS = conf.getLong("spark.siteMaster.timeout", 60) * 1000 / 4

  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  private val RECOVERY_MODE = conf.get("spark.deploy.siteMaster.recoveryMode", "NONE")

  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS =
    math.round(10 * REGISTRATION_RETRY_FUZZ_MULTIPLIER)
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS =
    math.round(60 * REGISTRATION_RETRY_FUZZ_MULTIPLIER)

  private val CLEANUP_ENABLED = conf.getBoolean("spark.siteMaster.cleanup.enabled", false)
  // How often worker will clean up old app folders
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.siteMaster.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.siteMaster.cleanup.appDataTtl", 7 * 24 * 3600)

  private val testing: Boolean = sys.props.contains("spark.testing")

  val workers = new HashSet[WorkerInfo]
  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  val siteApps = new HashSet[SiteAppInfo]
  val idToSiteApp = new HashMap[String, SiteAppInfo]
  private val endpointToSiteApp = new HashMap[RpcEndpointRef, SiteAppInfo]
  private val addressToSiteApp = new HashMap[RpcAddress, SiteAppInfo]
  private val waitingSiteApps = new ArrayBuffer[SiteAppInfo]
  private val completedSiteApps = new ArrayBuffer[SiteAppInfo]
  private var nextSiteAppNumber = 0

  private var globalMaster: Option[RpcEndpointRef] = None
  private var activeGlobalMasterUrl: String = ""
  private var activeGlobalMasterWebUiUrl: String = ""
  private var registered = false
  private var connected = false

  private var siteMasterWebUiUrl: String = ""
  private var siteMasterUrl = RpcEndpointAddress(rpcEnv.address, endpointName).toString

  private val siteMasterId = generateSiteMasterId()

  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  var workDir: File = _

  val globalDrivers = new HashMap[String, GlobalDriverRunner]
  val finishedGlobalDrivers = new LinkedHashMap[String, GlobalDriverRunner]
  val siteDrivers = new HashMap[String, SiteDriverRunner]  // fullId
  val finishedSiteDrivers = new LinkedHashMap[String, SiteDriverRunner]

  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]

  val retainedGlobalDrivers = conf.getInt("spark.siteMaster.ui.retainedGlobalDrivers",
    SiteMasterWebUI.DEFAULT_RETAINED_GLOBAL_DRIVERS)
  val retainedSiteDrivers = conf.getInt("spark.siteMaster.ui.retainedSiteDrivers",
    SiteMasterWebUI.DEFAULT_RETAINED_SITE_DRIVERS)

  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  private var webUi: SiteMasterWebUI = _

  private var connectionAttemptCount = 0

  private val metricsSystem = MetricsSystem.createMetricsSystem("siteMaster", conf, securityMgr)
  private val siteMasterSource = new SiteMasterSource(this)

  private var registerGlobalMasterFutures: Array[JFuture[_]] = _
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // thread pool to register with global master
  private val registerGlobalMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "site-master-register-global-master-threadpool",
    gmRpcAddresses.length
  )

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private var state = SiteMasterInState.STANDBY

  private var persistenceEngine: PersistenceEngine = _
  private var leaderElectionAgent: LeaderElectionAgent = _
  private var recoveryCompletionTask: JScheduledFuture[_] = _
  private var checkForWorkerTimeOutTask: JScheduledFuture[_] = _

  // 在cluster中为siteApp分配executor时, 是否默认扩散策略
  private val spreadOutApps = conf.getBoolean("spark.deploy.siteMaster.spreadOut", true)

  val reverseProxy = conf.getBoolean("spark.ui.siteMaster.reverseProxy", false)

  private def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def onStart(): Unit = {
    assert(!registered)
    logInfo("Starting Spark SiteMaster %s:%d with %d cores, %s RAM".format(
      host, port, cores, memory))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark Home: " + sparkHome)
    createWorkDir()
    webUi = new SiteMasterWebUI(this, webUiPort)
    webUi.bind()

    siteMasterWebUiUrl = s"http://$publicAddress:${webUi.boundPort}"

    checkForWorkerTimeOutTask = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError { self.send(CheckForWorkerTimeOut) }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    metricsSystem.registerSource(siteMasterSource)
    metricsSystem.start()
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def electedLeader(): Unit = self.send(ElectedLeader)

  override def revokedLeadership(): Unit = self.send(RevokedLeadership)

  override def receive: PartialFunction[Any, Unit] = {
    case SendHeartbeat =>
      if (connected) sendToGlobalMaster(SiteMasterHeartbeat(siteMasterId, self))

    case WorkerHeartbeat(workerId, workerRef) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          worker.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register")
            workerRef.send(ReconnectWorker(siteMasterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case ElectedLeader =>
      val (storedSiteApps, storedWorkers) = persistenceEngine.readSiteMasterPersistedData(rpcEnv)
      if (storedSiteApps.isEmpty && storedWorkers.isEmpty) {
        state = SiteMasterInState.ALIVE
        logInfo("I have been elected leader! New state: " + state)
        registerWithGlobalMaster()
      } else {
        state = SiteMasterInState.RECOVERING
        logInfo("I have been elected leader! New state: " + state)
      }
      if (state == SiteMasterInState.RECOVERING) {
        beginRecovery(storedSiteApps, storedWorkers)
        recoveryCompletionTask = forwardMessageScheduler.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError{
            self.send(CompleteRecovery)
          }}, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS )
      }

    case RevokedLeadership =>
      logError("Leadership has been revoked -- site master shutting down.")
      System.exit(0)

    case CompleteRecovery =>
      completeRecovery()

    case ReregisterWithGlobalMaster =>
      reregisterWithGlobalMaster()

    case WorkerLatestState(workerId, executors) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          for (exec <- executors) {
            val executorMatches = workerInfo.executors.exists {
              case (_, e) => e.siteApp.id == exec.siteAppId && e.id == exec.execId
            }
            if (!executorMatches) {
              workerInfo.endpoint.send(KillExecutor(siteMasterUrl, exec.siteAppId, exec.execId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }

    case LaunchSiteDriver(gmUrl, appId, sdriverId, appDesc, cores_, memory_) => // TODO-lzp
      if (gmUrl != activeGlobalMasterUrl) {
        logWarning("Invalid Global Master (" + gmUrl + ") attempted to launch sitedriver.")
      } else {
        val fullId = appId + "/" + sdriverId
        try {
          logInfo("Asked to launch site driver %s/%s for %s".format(appId, sdriverId, appDesc.name))

          val siteDriverDir = new File(workDir, fullId)
          if (!siteDriverDir.mkdirs()) {
            throw new IOException("Failed to create directory " + siteDriverDir)
          }

          val appLocalDirs = appDirectories.getOrElse(appId,
            Utils.getOrCreateLocalRootDirs(conf).map { dir =>
              val appDir = Utils.createDirectory(dir, namePrefix = "site-driver")
              Utils.chmod700(appDir)
              appDir.getAbsolutePath()
            }.toSeq)
          appDirectories(appId) = appLocalDirs
          val manager = new SiteDriverRunner(
            appId,
            sdriverId,
            appDesc.copy(command = Utils.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            siteMasterId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            siteDriverDir,
            siteMasterUrl,
            conf,
            appLocalDirs,
            SiteDriverState.RUNNING
          )
          siteDrivers(fullId) = manager
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          sendToGlobalMaster(SiteDriverStateChanged(appId, sdriverId, manager.state, None, None))
        } catch {
          case e: Exception =>
            logError(s"Failed to launch executor $fullId for ${appDesc.name}.", e)
            if (siteDrivers.contains(fullId)) {
              siteDrivers(fullId).kill()
              siteDrivers -= fullId
            }
            sendToGlobalMaster(SiteDriverStateChanged(appId, sdriverId, SiteDriverState.FAILED,
              Some(e.toString), None))
        }
      }

    // from gm
    case KillSiteDriver(gmUrl, appId, sdId) =>
      if (gmUrl != activeGlobalMasterUrl) {
        logWarning("Invalid Global Master (" + gmUrl + ") attempted to kill site driver " + sdId)
      } else {
        val fullId = s"$appId/$sdId"
        siteDrivers.get(fullId) match {
          case Some(sdriver) =>
            logInfo("Asked to kill site driver " + fullId)
            sdriver.kill()
          case None =>
            logInfo("Asked to kill unknown site driver " + fullId)
        }
      }

    case LaunchGlobalDriver(gdriverId, gdriverDesc) =>
      logInfo(s"Asked to launch global driver $gdriverId")
      val gdriver = new GlobalDriverRunner(
        conf,
        gdriverId,
        workDir,
        sparkHome,
        gdriverDesc.copy(command = Utils.maybeUpdateSSLSettings(gdriverDesc.command, conf)),
        self,
        siteMasterUrl,
        securityMgr
      )
      globalDrivers(gdriverId) = gdriver
      gdriver.start()

      coresUsed += gdriverDesc.cores
      memoryUsed += gdriverDesc.mem

    case KillGlobalDriver(gdId) =>
      logInfo(s"Asked to kill global driver $gdId")
      globalDrivers.get(gdId) match {
        case Some(gd) =>
          logInfo(s"Asked to kill global driver $gdId")
          gd.kill()
        case None =>
          logError(s"Asked to kill unknown global driver $gdId")
      }

    case siteDriverStateChanged @ SiteDriverStateChanged(appId, sdId, state_, msg, exitStatus) =>
      handleSiteDriverStateChanged(siteDriverStateChanged)

    case globalDriverStateChanged @ GlobalDriverStateChanged(gdId, state_, exception) =>
      handleGlobalDriverStateChanged(globalDriverStateChanged)

    // TODO-lzp:
    case ExecutorStateChanged =>

    // gm -> sm: GlobalMasterChanged, when gm recovery
    case GlobalMasterChanged(gmRef, gmWebUiUrl) =>
      logInfo("Global Master has changed, new global Master is at ")
      changeGlobalMaster(gmRef, gmWebUiUrl)
      val sdrivers = siteDrivers.values.map { sd =>  // SiteDriverRunner
        new SiteDriverDescription(sd.appId, sd.sdId, sd.cores, sd.state)
      }
      gmRef.send(SiteMasterSchedulerStateResponse(siteMasterId,
        sdrivers.toList, globalDrivers.keys.toSeq))

    // self-send
    case WorkDirCleanup =>
      val appIds = siteDrivers.values.map(_.appId).toSet
      val cleanupFuture = concurrent.Future {
        val appDirs = workDir.listFiles()
        if  (appDirs == null) {
          throw new IOException("ERROR: Failed to list files in " + appDirs)
        }
        appDirs.filter { dir =>
          var appIdFromDir = dir.getName
          val isAppStillRunning = appIds.contains(appIdFromDir)
          // is dir && is not running && doesn't contain new file
          dir.isDirectory && !isAppStillRunning &&
            !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
        }.foreach { dir =>
          logInfo(s"Removing directory: ${dir.getPath}")
          Utils.deleteRecursively(dir)
        }
      }(cleanupThreadExecutor)

      cleanupFuture.onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
      }(cleanupThreadExecutor)

    case ReconnectSiteMaster(gmUrl) =>
      logInfo(s"Global Master with url $gmUrl requested this site master to reconnect.")
      registerWithGlobalMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(id))
      }
      maybeCleanupApplication(id)

    case RegisterSiteApplication(siteAppDescription, sdriver) => // TODO-lzp
      if (state == SiteMasterInState.STANDBY) {

      } else {
        logInfo("Registering site app " + siteAppDescription.name)
        val siteApp = createSiteApplication(siteAppDescription, sdriver)
        registerSiteApplication(siteApp)
        logInfo("Registered site app " + siteAppDescription.name + " with ID " + siteApp.id)
        persistenceEngine.addSiteApp(siteApp)
        sdriver.send(RegisteredApplication(siteApp.id, self))
      }

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case SiteMasterChangeAcknowledged(siteAppId) =>
      idToSiteApp.get(siteAppId) match {
        case Some(siteApp) =>
          logInfo("SiteApplication has been re-registered: " + siteAppId)
          siteApp.state = SiteAppState.WAITING
        case None =>
          logWarning("Site Master change ack from unknown app: " + siteAppId)
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterWorker(id, host, port, ref, cores, memory, webUiUrl) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        host, port, cores, memory
      ))
      if (state == SiteMasterInState.STANDBY) {
        context.reply(SiteMasterInStandby)
      } else if (idToWorker.contains(id)) {
        context.reply(RegisterWorkerFailed("Duplicate worker id"))
      } else {
        val worker = new WorkerInfo(id, host, port, cores, memory, ref, webUiUrl)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          context.reply(RegisteredWorker(self, siteMasterWebUiUrl))
          schedule()
        } else {
          val workerAddress = ref.address
          logWarning("Worker registration failed. Attempt to re-register worker at same " +
            "address: " + workerAddress)
          context.reply(RegisterWorkerFailed("Attempted to re-register worker at same address: " +
            workerAddress))
        }
      }

    case RequestSiteMasterState =>
      context.reply(SiteMasterStateResponse(host, port, siteMasterId, activeGlobalMasterUrl,
        activeGlobalMasterWebUiUrl, cores, memory, coresUsed, memoryUsed, workers.toArray,
        siteApps.toArray, completedSiteApps.toArray,
        siteDrivers.values.toList, finishedSiteDrivers.values.toList,
        globalDrivers.values.toList, finishedGlobalDrivers.values.toList,
        state
      ))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(port, webUi.boundPort))
  }

  // TODO-lzp
  private def handleSiteDriverStateChanged(changed: SiteDriverStateChanged): Unit = {

  }

  // 此时, GlobalDriver已经结束, 无论因为什么原因. 此处根据不同的结束原因, 在SiteMaster上记录下日志
  private def handleGlobalDriverStateChanged(changed: GlobalDriverStateChanged): Unit = {
    val gdriverId = changed.gdriverId
    val exception = changed.exception
    val state = changed.state
    state match {
      case GlobalDriverState.ERROR =>
        logWarning(s"Global Driver $gdriverId failed with unrecoverable exception: " +
                     s"${exception.get}")
      case GlobalDriverState.FAILED =>
        logWarning(s"Global Driver $gdriverId exited with failure")
      case GlobalDriverState.FINISHED =>
        logInfo(s"Global Driver $gdriverId exited successfully")
      case GlobalDriverState.KILLED =>
        logInfo(s"Global Driver $gdriverId was killed by user")
      case _ =>
        logDebug(s"Global Driver $gdriverId changed state to $state")
    }
    sendToGlobalMaster(changed)
    val gdriver = globalDrivers.remove(gdriverId).get
    finishedGlobalDrivers(gdriverId) = gdriver
    trimFinishedGlobalDriversIfNecessary()  // 移除旧的gd, 因为能保留的数目有限
    memoryUsed -= gdriver.gdriverDesc.mem
    coresUsed -= gdriver.gdriverDesc.cores
  }

  // TODO-lzp
  private def schedule(): Unit = {

  }

  private def maybeCleanupApplication(appId: String): Unit = {
    val shouldCleanup = finishedApps.contains(appId) && !siteDrivers.values.exists(_.appId == appId)
    if (shouldCleanup) {
      finishedApps -= appId
      appDirectories.remove(appId).foreach { dirList =>
        logInfo(s"Cleanuping up local directories for application $appId")
        dirList.foreach { dir =>
          Utils.deleteRecursively(new File(dir))
        }
      }
    }
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // clean the dead worker
    workers.filter { w =>
      w.host == worker.host && w.port == worker.port && w.state == WorkerState.DEAD
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker

    if (reverseProxy) {
      webUi.addProxyTargets(worker.id, worker.webUiAddress)
    }
    true
  }

  private def removeWorker(worker: WorkerInfo): Unit = {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address
    if (reverseProxy) webUi.removeProxyTargets(worker.id)
    for (exec <- worker.executors.values) {
      logInfo("Telling site app of lost executor: " + exec.id)
      exec.siteApp.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true
      ))
      exec.state = ExecutorState.LOST
      exec.siteApp.removeExecutor(exec)
    }
    persistenceEngine.removeWorker(worker)
  }

  private def sendToGlobalMaster(msg: Any): Unit = {
    globalMaster match {
      case Some(gmRef) => gmRef.send(msg)
      case None =>
        logWarning(s"Dropping $msg because the connection to global master has not " +
          "yet been established")
    }
  }

  private def reregisterWithGlobalMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to global master (attempt # $connectionAttemptCount)")
        globalMaster match {
          case Some(gmRef) =>
            if (registerGlobalMasterFutures != null) {
              registerGlobalMasterFutures.foreach(_.cancel(true))
            }
            val gmAddress = gmRef.address
            registerGlobalMasterFutures = Array(registerGlobalMasterThreadPool.submit(
              new Runnable {
                override def run(): Unit = {
                  try {
                    logInfo("Connecting to global master " + gmAddress + " ...")
                    val gmEndpoint = rpcEnv.setupEndpointRef(gmAddress, GlobalMaster.ENDPOINT_NAME)
                    registerWithGlobalMaster(gmEndpoint)
                  } catch {
                    case ie: InterruptedException => // Cancelled
                    case NonFatal(e) => logWarning(s"Failed to connect to master $gmAddress", e)
                  }
                }
              }
            ))
          case None =>
            if (registerGlobalMasterFutures != null) {
              registerGlobalMasterFutures.foreach(_.cancel(true))
            }
            tryRegisterAllGlobalMasters()
        }
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(ReregisterWithGlobalMaster)
            }}, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
            PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
            TimeUnit.MILLISECONDS)
          )
        }
      } else {
        logError("All global masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  private def registerWithGlobalMaster(): Unit = {
    registrationRetryTimer match {
      case None =>
        registered = false
        registerGlobalMasterFutures = tryRegisterAllGlobalMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwardMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithGlobalMaster))
            }}, INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS
        ))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the global master," +
          "since there is an attempt scheduled already.")
    }
  }

  private def tryRegisterAllGlobalMasters(): Array[JFuture[_]] = {
    gmRpcAddresses.map {gmAddress =>
      registerGlobalMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to global master " + gmAddress + " ...")
            val gmEndpoint = rpcEnv.setupEndpointRef(gmAddress, GlobalMaster.ENDPOINT_NAME)
            registerWithGlobalMaster(gmEndpoint)
          } catch {
            case ie: InterruptedException =>
            case NonFatal(e) => logWarning(s"Failed to connect to global master $gmAddress", e)
          }
        }
      })
    }
  }

  private def registerWithGlobalMaster(gmEndpoint: RpcEndpointRef): Unit = {
    gmEndpoint.ask[RegisterSiteMasterResponse](RegisterSiteMaster(
      siteMasterId, host, port, self, cores, memory, siteMasterWebUiUrl
    )).onComplete {
      case Success(msg) =>
        Utils.tryLogNonFatalError { handleRegisterResponse(msg) }
      case Failure(e) =>
        logError(s"Cannot register with master: ${gmEndpoint.address}", e)
        System.exit(1)
    }(ThreadUtils.sameThread)
  }

  def createSiteApplication(desc: SiteAppDescription, driver: RpcEndpointRef): SiteAppInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val siteAppId = newSiteAppId(date)
    new SiteAppInfo(now, siteAppId, desc, date, driver)
  }

  private def newSiteAppId(date: Date): String = {
    val siteAppId = "site-app-%s-%04d".format(createDateFormat.format(date), nextSiteAppNumber)
    nextSiteAppNumber += 1
    siteAppId
  }

  // TODO-lzp
  private def registerSiteApplication(siteApp: SiteAppInfo): Unit = {

  }

  private def handleRegisterResponse(msg: RegisterSiteMasterResponse): Unit = synchronized {
    msg match {
      case RegisteredSiteMaster(gmRef, gmWebUiUrl) =>
        logInfo("Successfully registered with global master " + gmRef.address.toSparkURL)
        registered = true
        changeGlobalMaster(gmRef, gmWebUiUrl)
        forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS )
        if (CLEANUP_ENABLED) {
          logInfo("Site Master cleanup enabled; " +
            s"old application directories will be deleted in: $workDir")
          forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(WorkDirCleanup)
            }}, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS
          )
        }

        val sdrivers = siteDrivers.values.map { sd =>  // SiteDriverRunner
          new SiteDriverDescription(sd.appId, sd.sdId, sd.cores, sd.state)
        }
        gmRef.send(SiteMasterLatestState(siteMasterId, sdrivers.toList, globalDrivers.keys.toSeq))

      case RegisterSiteMasterFailed(msg) =>
        if (!registered) {
          logError("Site Master registration failed: " + msg)
          System.exit(1)
        }

      case GlobalMasterInStandby =>  // do nothing

    }
  }

  private def changeGlobalMaster(gmRef: RpcEndpointRef, gmWebUiUrl: String): Unit = {
    activeGlobalMasterUrl = gmRef.address.toSparkURL
    activeGlobalMasterWebUiUrl = gmWebUiUrl
    globalMaster = Some(gmRef)
    connected = true
    if (conf.getBoolean("spark.ui.globalMaster.reverseProxy", false)) {
      logInfo(s"WorkerWebUI is available at $activeGlobalMasterWebUiUrl/proxy/$siteMasterId")
    }
    cancelLastRegistrationRetry()
  }

  private def cancelLastRegistrationRetry(): Unit = {
    if (registerGlobalMasterFutures != null) {
      registerGlobalMasterFutures.foreach(_.cancel(true))
      registerGlobalMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  private def completeRecovery(): Unit = {
    if (state != SiteMasterInState.RECOVERING) return
    state = SiteMasterInState.COMPLETING_RECOVERY

    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    siteApps.filter(_.state == SiteAppState.UNKNOWN).foreach(finishSiteApp)

    state = SiteMasterInState.ALIVE

    registerWithGlobalMaster()  // register gm after complete recovery

    schedule()
    logInfo("Recovery complete - resuming operation")
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  //  private def scheduleExecutorsOnWorkers(
  //                                          app: ApplicationInfo,
  //                                          usableWorkers: Array[WorkerInfo],
  //                                          spreadOutApps: Boolean): Array[Int] = {
  //    val coresPerExecutor = app.desc.coresPerExecutor
  //    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
  //    val oneExecutorPerWorker = coresPerExecutor.isEmpty
  //    val memoryPerExecutor = app.desc.memoryPerExecutorMB
  //    val numUsable = usableWorkers.length
  //    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
  //    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
  //    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
  //
  //    /** Return whether the specified worker can launch an executor for this app. */
  //    def canLaunchExecutor(pos: Int): Boolean = {
  //      val keepScheduling = coresToAssign >= minCoresPerExecutor
  //      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor
  //
  //      // If we allow multiple executors per worker, then we can always launch new executors.
  //      // Otherwise, if there is already an executor on this worker, just give it more cores.
  //      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
  //      if (launchingNewExecutor) {
  //        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
  //        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
  //        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
  //        keepScheduling && enoughCores && enoughMemory && underLimit
  //      } else {
  //        // We're adding cores to an existing executor, so no need
  //        // to check memory and executor limits
  //        keepScheduling && enoughCores
  //      }
  //    }
  //
  //    // Keep launching executors until no more workers can accommodate any
  //    // more executors, or if we have reached this application's limits
  //    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
  //    while (freeWorkers.nonEmpty) {
  //      freeWorkers.foreach { pos =>
  //        var keepScheduling = true
  //        while (keepScheduling && canLaunchExecutor(pos)) {
  //          coresToAssign -= minCoresPerExecutor
  //          assignedCores(pos) += minCoresPerExecutor
  //
  //          // If we are launching one executor per worker, then every iteration assigns 1 core
  //          // to the executor. Otherwise, every iteration assigns cores to a new executor.
  //          if (oneExecutorPerWorker) {
  //            assignedExecutors(pos) = 1
  //          } else {
  //            assignedExecutors(pos) += 1
  //          }
  //
  //          // Spreading out an application means spreading out its executors across as
  //          // many workers as possible. If we are not spreading out, then we should keep
  //          // scheduling executors on this worker until we use all of its resources.
  //          // Otherwise, just move on to the next worker.
  //          if (spreadOutApps) {
  //            keepScheduling = false
  //          }
  //        }
  //      }
  //      freeWorkers = freeWorkers.filter(canLaunchExecutor)
  //    }
  //    assignedCores
  //  }

  /**
   * Schedule and launch executors on workers
   */
  //  private def startExecutorsOnWorkers(): Unit = {
  //    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
  //    // in the queue, then the second app, etc.
  //    for (app <- waitingApps if app.coresLeft > 0) {
  //      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
  //      // Filter out workers that don't have enough resources to launch an executor
  //      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
  //        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
  //          worker.coresFree >= coresPerExecutor.getOrElse(1))
  //        .sortBy(_.coresFree).reverse
  //      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
  //
  //      // Now that we've decided how many cores to allocate on each worker, let's allocate them
  //      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
  //        allocateWorkerResourceToExecutors(
  //          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
  //      }
  //    }
  //  }

  //  /**
  //   * Allocate a worker's resources to one or more executors.
  //   * @param app the info of the application which the executors belong to
  //   * @param assignedCores number of cores on this worker for this application
  //   * @param coresPerExecutor number of cores per executor
  //   * @param worker the worker info
  //   */
  //  private def allocateWorkerResourceToExecutors(
  //                                                 app: ApplicationInfo,
  //                                                 assignedCores: Int,
  //                                                 coresPerExecutor: Option[Int],
  //                                                 worker: WorkerInfo): Unit = {
  //    // If the number of cores per executor is specified, we divide the cores assigned
  //    // to this worker evenly among the executors with no remainder.
  //    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
  //    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
  //    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
  //    for (i <- 1 to numExecutors) {
  //      val exec = app.addExecutor(worker, coresToAssign)
  //      launchExecutor(worker, exec)
  //      app.state = ApplicationState.RUNNING
  //    }
  //  }

  // TODO-lzp
  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)

  }

  private def finishSiteApp(siteApp: SiteAppInfo): Unit = {
    removeSiteApp(siteApp, SiteAppState.FINISHED)
  }

  // TODO-lzp
  private def removeSiteApp(siteApp: SiteAppInfo, state: SiteAppState): Unit = {

  }

  private def beginRecovery(
    storedSiteApps: Seq[SiteAppInfo],
    storedWorkers: Seq[WorkerInfo]): Unit = {
    for (sapp <- storedSiteApps) {
      logInfo("Trying to recover site app: " + sapp.id)
      try {
        registerSiteApp(sapp)
        sapp.state = SiteAppState.UNKNOWN
        // TODO-lzp: what's the site app's driver, do what?
        sapp.driver.send(SiteMasterChanged(self, siteMasterWebUiUrl))
      } catch {
        case e: Exception =>
          logInfo("Site App " + sapp.id + " had exception on reconnect")
      }
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(SiteMasterChanged(self, siteMasterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  // TODO-lzp
  private def registerSiteApp(sapp: SiteAppInfo): Unit = {

  }

  private def generateSiteMasterId(): String = {
    "site-master-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop(): Unit = {
    metricsSystem.report()
    if (recoveryCompletionTask != null) recoveryCompletionTask.cancel(true)
    if (checkForWorkerTimeOutTask != null) checkForWorkerTimeOutTask.cancel(true)
    cancelLastRegistrationRetry()
    forwardMessageScheduler.shutdownNow()
    cleanupThreadExecutor.shutdownNow()
    registerGlobalMasterThreadPool.shutdownNow()
    globalDrivers.values.foreach(_.kill())
    siteDrivers.values.foreach(_.kill())
    webUi.stop()
    metricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  private def timeOutDeadWorkers(): Unit = {
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000
        ))
        removeWorker(worker)
      } else {  // the state is DEAD, the worker has been removed, just keep in workers
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker
        }
      }
    }
  }

  private def trimFinishedGlobalDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedGlobalDrivers.size > retainedGlobalDrivers) {
      finishedGlobalDrivers.take(math.max(finishedGlobalDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedGlobalDrivers.remove(driverId)
      }
    }
  }

  private def canCompleteRecovery =
    !workers.exists(_.state == WorkerState.UNKNOWN) &&
      !siteApps.exists(_.state == SiteAppState.UNKNOWN)

  override def onDisconnected(address: RpcAddress): Unit = {
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker)
    addressToSiteApp.get(address).foreach(finishSiteApp)
    if (state == SiteMasterInState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }
}

private[deploy] object SiteMaster extends Logging{
  val SYSTEM_NAME = "sparkSiteMaster"
  val ENDPOINT_NAME = "SiteMaster"

  def main(argString: Array[String]): Unit = {
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new SiteMasterArguments(argString, conf)

    val (rpcEnv, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort,
      args.cores, args.memory, args.gmasters, args.workDir, conf)
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    gmUrls: Array[String],
    workDir: String,
    conf: SparkConf): (RpcEnv, Int) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val gmAddresses = gmUrls.map(RpcAddress.fromSparkURL)
    val smEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME, new SiteMaster(rpcEnv,
      webUiPort, cores, memory, gmAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
    val portResponse = smEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portResponse.webUIPort)
  }
}
