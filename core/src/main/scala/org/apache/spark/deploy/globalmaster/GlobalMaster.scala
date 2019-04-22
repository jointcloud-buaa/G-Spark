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

package org.apache.spark.deploy.globalmaster

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, GlobalDriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.globalmaster.GlobalDriverState.GlobalDriverState
import org.apache.spark.deploy.globalmaster.GlobalMasterMessages._
import org.apache.spark.deploy.globalmaster.ui.GlobalMasterWebUI
import org.apache.spark.deploy.leaderandrecovery._
import org.apache.spark.deploy.leaderandrecovery.LeaderMessages.{ElectedLeader, RevokedLeadership}
import org.apache.spark.deploy.leaderandrecovery.RecoveryMessages.CompleteRecovery
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.deploy.sitemaster.SiteMaster
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{ThreadUtils, Utils}

private[deploy] class GlobalMaster(
  override val rpcEnv: RpcEnv,
  address: RpcAddress,
  webUiPort: Int,
  val securityMgr: SecurityManager,
  val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("global-master-forward-message-thread")

  // TODO-maybedel
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private val SITE_MASTER_TIMEOUT_MS = conf.getLong("spark.siteMaster.timeout", 60) * 1000
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  private val RETAINED_GLOBAL_DRIVERS = conf.getInt("spark.deploy.retainedGlobalDrivers", 200)
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.siteMaster.persistence", 15)
  private val RECOVERY_MODE = conf.get("spark.deploy.globalMaster.recoveryMode", "NONE")
  private val MAX_SITE_DRIVER_RETRIES = conf.getInt("spark.deploy.maxSiteDriverRetries", 10)

  val siteMasters = new HashSet[SiteMasterInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToSiteMaster = new HashMap[String, SiteMasterInfo]
  private val idToClusterName = new HashMap[String, String]
  private val addressToSiteMaster = new HashMap[RpcAddress, SiteMasterInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0

  private val globalDrivers = new HashSet[GlobalDriverInfo]
  private val completedGlobalDrivers = new ArrayBuffer[GlobalDriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingGlobalDrivers = new ArrayBuffer[GlobalDriverInfo]
  private var nextGlobalDriverNumber = 0

  Utils.checkHost(address.host, "Expected hostname")

  private val masterMetricsSystem =
    MetricsSystem.createMetricsSystem("globalMaster", conf, securityMgr)
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  private val masterSource = new GlobalMasterSource(this)

//   After onStart, webUi will be set
  private var webUi: GlobalMasterWebUI = null

  private val globalMasterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val globalMasterUrl = address.toSparkURL
  private var globalMasterWebUiUrl: String = _

  private var state = GlobalMasterState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForMasterTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  // 考虑到, 在可用的siteMaster上都会启动siteDriver, 则此设置无用
//  private val spreadOutApps = conf.getBoolean("spark.deploy.globalMaster.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  val reverseProxy = conf.getBoolean("spark.ui.globalMaster.reverseProxy", false)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.globalMaster.rest.enabled", true)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + globalMasterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    webUi = new GlobalMasterWebUI(this, webUiPort)
    webUi.bind()
    globalMasterWebUiUrl = "http://" + globalMasterPublicAddress + ":" + webUi.boundPort
    if (reverseProxy) {
      globalMasterWebUiUrl = conf.get("spark.ui.reverseProxyUrl", globalMasterWebUiUrl)
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
        s"Applications UIs are available at $globalMasterWebUiUrl")
    }
    checkForMasterTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForSiteMasterTimeOut)
      }
    }, 0, SITE_MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    if (restServerEnabled) {
      val port = conf.getInt("spark.globalMaster.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, globalMasterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

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
        val clazz = Utils.classForName(conf.get("spark.deploy.globalMaster.recoveryMode.factory"))
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

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForMasterTimeOutTask != null) {
      checkForMasterTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      val (storedApps, storedDrivers, storedSiteMasters) =
        persistenceEngine.readGlobalMasterPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedSiteMasters.isEmpty) {
        GlobalMasterState.ALIVE
      } else {
        GlobalMasterState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == GlobalMasterState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedSiteMasters)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, SITE_MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)

      // from ClientEndpoint
    case RegisterApplication(description, driver) =>  // appClient
      // TODO Prevent repeated registrations from some driver
      if (state == GlobalMasterState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        val app = createApplication(description, driver)
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        persistenceEngine.addApplication(app)
        driver.send(RegisteredApplication(app.id, self))
        schedule()
      }

      // from SiteMaster
      // 更新ApplicationInfo中存储的关于SiteDriver's状态
    case SiteDriverStateChanged(appId, sdriverId, state, message, exitStatus) =>
      val sdriverOption = idToApp.get(appId).flatMap(app => app.siteDrivers.get(sdriverId))
      sdriverOption match {
        case Some(sd) =>  // SiteDriverDesc
          val appInfo = idToApp(appId)
          val oldState = sd.state
          sd.state = state

          if (state == SiteDriverState.RUNNING) {
            assert(oldState == SiteDriverState.LAUNCHING,
              s"site driver $sdriverId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }

          sd.application.driver.send(
            SiteDriverUpdated(sdriverId, state, message, exitStatus, false))

          if (SiteDriverState.isFinished(state)) {
            logInfo(s"Removing site driver ${sd.fullId} because it is $state")
            if (!appInfo.isFinished) {
              appInfo.removeSiteDriver(sd)
            }
            sd.master.removeSiteDriver(sd)

            val normalExit = exitStatus == Some(0)
            if (!normalExit
              && appInfo.incrementRetryCount() >= MAX_SITE_DRIVER_RETRIES
              && MAX_SITE_DRIVER_RETRIES >= 0) {
              val sdrivers = appInfo.siteDrivers.values
              if (!sdrivers.exists(_.state == SiteDriverState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(s"Got status update for unknown site driver $appId/$sdriverId")
      }

    case GlobalDriverStateChanged(gdriverId, state, exception) =>
      state match {
        case GlobalDriverState.ERROR | GlobalDriverState.FINISHED
             | GlobalDriverState.KILLED | GlobalDriverState.FAILED =>
          removeGlobalDriver(gdriverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $gdriverId: $state")
      }

    case SiteMasterHeartbeat(smId, smRef) =>
      idToSiteMaster.get(smId) match {
        case Some(smasterInfo) =>
          smasterInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (siteMasters.map(_.id).contains(smId)) {
            logWarning(s"Got heartbeat from unregistered site master $smId." +
              " Asking it to re-register.")
            smRef.send(ReconnectSiteMaster(globalMasterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered site master $smId." +
              " This site master was never registered, so ignoring the heartbeat.")
          }
      }

      // gm -> app client : GlobalMasterChanged
    case GlobalMasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Global Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }

      // gm -> sm : GlobalMasterChange, just for recovery
    case SiteMasterSchedulerStateResponse(smId, sdrivers, gdriverIds) =>
      idToSiteMaster.get(smId) match {
        case Some(smInfo) =>
          logInfo("Site Master has been re-registered: " + smId)
          smInfo.state = SiteMasterOutState.ALIVE

          val validSiteDrivers = sdrivers.filter(sdriver => idToApp.get(sdriver.appId).isDefined)
          for (sdDescL <- validSiteDrivers) {  // SiteDriverDescription
            val app = idToApp(sdDescL.appId)
            val sdDesc = app.addSiteDriver(smInfo, sdDescL.cores, Some(sdDescL.sdriverId))
            smInfo.addSiteDriver(sdDesc)
            sdDesc.copyState(sdDescL)
          }

          for (gdriverId <- gdriverIds) {
            globalDrivers.find(_.id == gdriverId).foreach { gdriver =>
              gdriver.siteMaster = Some(smInfo)
              gdriver.state = GlobalDriverState.RUNNING
              smInfo.globalDrivers(gdriverId) = gdriver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown site master: " + smId)
      }

      if (canCompleteRecovery) { completeRecovery() }

      // Site Master -> GlobalMaster : RegisterSiteMaster
      // Global Master -> Site Master : RegisteredSiteMaster
      // SiteMaster -> GlobalMaster : SiteMasterLatestState
    case SiteMasterLatestState(smId, siteDrivers, gdriverIds) =>
      idToSiteMaster.get(smId) match {
        case Some(smInfo) =>
          for (sd <- siteDrivers) {
            val sdriverMatches = smInfo.siteDrivers.values.exists { sdDesc =>
              sdDesc.application.id == sd.appId && sdDesc.id == sd.sdriverId
            }
            if (!sdriverMatches) {
              smInfo.endpoint.send(KillSiteDriver(globalMasterUrl, sd.appId, sd.sdriverId))
            }
          }

          for (gdId <- gdriverIds) {
            val gdMatches = smInfo.globalDrivers.values.exists(_.id == gdId)
            if (!gdMatches) {
              smInfo.endpoint.send(KillGlobalDriver(gdId))
            }
          }
        case None =>
          logWarning("Site Master state from unknown site master: " + smId)
      }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

    case CheckForSiteMasterTimeOut =>
      timeOutDeadMasters()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // SiteMaster -> Global Master
    case RegisterSiteMaster(
      id, smHost, smPort, clusterName, smRef, cores, memory, smWebUiUrl) =>
      logInfo("Registering site master %s:%d with %d cores, %s RAM".format(
        smHost, smPort, cores, Utils.megabytesToString(memory)))
      if (state == GlobalMasterState.STANDBY) {
        context.reply(GlobalMasterInStandby)
      } else if (idToSiteMaster.contains(id)) {
        context.reply(RegisterSiteMasterFailed("Duplicate worker ID"))
      } else {
        val smaster = new SiteMasterInfo(
          id, smHost, smPort, clusterName, cores, memory, smRef, smWebUiUrl)
        if (registerSiteMaster(smaster)) {
          persistenceEngine.addSiteMaster(smaster)
          context.reply(RegisteredSiteMaster(self, globalMasterWebUiUrl))
          schedule()
        } else {
          val masterAddress = smaster.endpoint.address
          logWarning("Site Master registration failed. Attempted to re-register worker at same " +
            "address: " + masterAddress)
          context.reply(RegisterSiteMasterFailed("Attempted to re-register worker at same address: "
            + masterAddress))
        }
      }

      // from RestServer
    case RequestSubmitGlobalDriver(description) =>
      if (state != GlobalMasterState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitGlobalDriverResponse(self, false, None, msg))
      } else {
        // rest submit: DriverWrapper
        logInfo("Global Driver submitted " + description.command.mainClass)
        val gdriver = createGlobalDriver(description)
        persistenceEngine.addGlobalDriver(gdriver)
        waitingGlobalDrivers += gdriver
        globalDrivers.add(gdriver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitGlobalDriverResponse(self, true, Some(gdriver.id),
          s"Global Driver successfully submitted as ${gdriver.id}"))
      }

      // from WebUI, or RestServer
    case RequestKillDriver(driverId) =>
      if (state != GlobalMasterState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillGlobalDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill global driver " + driverId)
        val driver = globalDrivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingGlobalDrivers.contains(d)) {
              waitingGlobalDrivers -= d
              self.send(GlobalDriverStateChanged(driverId, GlobalDriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.siteMaster.foreach { sm =>
                sm.endpoint.send(KillGlobalDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillGlobalDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Global Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillGlobalDriverResponse(self, driverId, success = false, msg))
        }
      }

      // from RestServer
    case RequestDriverStatus(driverId) =>
      if (state != GlobalMasterState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          GlobalDriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (globalDrivers ++ completedGlobalDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(GlobalDriverStatusResponse(found = true, Some(driver.state),
              driver.siteMaster.map(_.id), driver.siteMaster.map(_.hostPort), driver.exception))
          case None =>
            context.reply(GlobalDriverStatusResponse(found = false, None, None, None, None))
        }
      }

      // from WebUI
    case RequestMasterState =>
      context.reply(GlobalMasterStateResponse(
        address.host, address.port, restServerBoundPort,
        siteMasters.toArray, apps.toArray, completedApps.toArray,
        globalDrivers.toArray, completedGlobalDrivers.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

      // do nothing
    case RequestExecutors(appId, requestedTotal) =>
//      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
//      val formattedExecutorIds = formatExecutorIds(executorIds)
//      context.reply(handleKillExecutors(appId, formattedExecutorIds))

    case GetSiteDriverIdsForApp(appId: String) =>
      val sdriverIds = idToApp.get(appId) match {
        case Some(app) =>
          app.siteDrivers.keys.toArray
        case None =>
          Array.empty[String]
      }
      context.reply(SiteDriverIdsForAppResponse(appId, sdriverIds))
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToSiteMaster.get(address).foreach(removeMaster)
    addressToApp.get(address).foreach(finishApplication)
    if (state == GlobalMasterState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  // dependent on the sitemasters and apps' state
  private def canCompleteRecovery =
    siteMasters.count(_.state == SiteMasterOutState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo],
                            storedDrivers: Seq[GlobalDriverInfo],
                            storedSiteMasters: Seq[SiteMasterInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver.send(GlobalMasterChanged(self, globalMasterWebUiUrl))  // send to appClient
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      globalDrivers += driver
    }

    for (smaster <- storedSiteMasters) {
      logInfo("Trying to recover site master: " + smaster.id)
      try {
        registerSiteMaster(smaster)
        smaster.state = SiteMasterOutState.UNKNOWN
        smaster.endpoint.send(GlobalMasterChanged(self, globalMasterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Site Master " + smaster.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    if (state != GlobalMasterState.RECOVERING) { return }
    state = GlobalMasterState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    siteMasters.filter(_.state == SiteMasterOutState.UNKNOWN).foreach(removeMaster)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    globalDrivers.filter(_.siteMaster.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeGlobalDriver(d.id, GlobalDriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = GlobalMasterState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule(): Unit = {
    if (state != GlobalMasterState.ALIVE) {
      return
    }
    val shuffledAliveSiteMasters = Random.shuffle(
      siteMasters.toSeq.filter(_.state == SiteMasterOutState.ALIVE))
    val numSiteMastersAlive = shuffledAliveSiteMasters.size
    var curPos = 0
    // 为等待列表中的globalDriver, 分配siteMaster去执行
    for (gdriver <- waitingGlobalDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numSiteMastersVisited = 0  // 保证对每个globaldriver只会访问一遍
      while (numSiteMastersVisited < numSiteMastersAlive && !launched) {
        val sm = shuffledAliveSiteMasters(curPos)
        numSiteMastersVisited += 1
        if (sm.memoryFree >= gdriver.desc.mem && sm.coresFree >= gdriver.desc.cores) {
          launchGlobalDriver(sm, gdriver)
          waitingGlobalDrivers -= gdriver
          launched = true
        }
        curPos = (curPos + 1) % numSiteMastersAlive
      }
    }
    startSiteDriverOnSiteMaster()
  }

  // 很简单, 每个SiteMaster, 在资源允许的情况下, 都要启动一个SiteDriver
  private def startSiteDriverOnSiteMaster(): Unit = {
    for (app <- waitingApps) {
      val coresPerSiteDriver = app.desc.coresPerSiteDriver
      val usableSiteMasters = siteMasters.toArray.filter(sm =>
        sm.state == SiteMasterOutState.ALIVE &&
        !sm.hasSiteDriver(app.id) &&  // the siteMaster has not a siteDriver for the app
        sm.memoryFree >= app.desc.memoryPerSiteDriverMB &&
        sm.coresFree >= coresPerSiteDriver.getOrElse(2)
      )
      for (smaster <- usableSiteMasters) {
        // TODO-lzp: the cores 2 is hard code
        val sdriver = app.addSiteDriver(smaster, coresPerSiteDriver.getOrElse(2))
        launchSiteDriver(smaster, sdriver)
        app.state = ApplicationState.RUNNING
      }
    }
  }

  private def launchSiteDriver(sm: SiteMasterInfo, sdDesc: SiteDriverDesc): Unit = {
    logInfo("Launching site driver " + sdDesc.fullId + " on site master " + sm.id)
    sm.addSiteDriver(sdDesc)
    sm.endpoint.send(LaunchSiteDriver(globalMasterUrl,
      sdDesc.application.id, sdDesc.id, sdDesc.application.desc, sdDesc.cores, sdDesc.memory))
    sdDesc.application.driver.send(
      SiteDriverAdded(sdDesc.id, sm.id, sm.hostPort, sdDesc.cores, sdDesc.memory))
  }

  private def registerSiteMaster(master: SiteMasterInfo): Boolean = {
    // lazy del dead master from siteMasters
    siteMasters.filter { sm =>
      (sm.host == master.host && sm.port == master.port) && (sm.state == SiteMasterOutState.DEAD)
    }.foreach { sm =>
      siteMasters -= sm
    }

    val masterAddress = master.endpoint.address
    if (addressToSiteMaster.contains(masterAddress)) {
      val oldMaster = addressToSiteMaster(masterAddress)
      if (oldMaster.state == SiteMasterOutState.UNKNOWN) {
        removeMaster(oldMaster)
      } else {
        logInfo("Attempted to re-register site master at same address: " + masterAddress)
        return false
      }
    }

    siteMasters += master
    idToSiteMaster(master.id) = master
    idToClusterName(master.id) = master.clusterName
    addressToSiteMaster(masterAddress) = master
    if (reverseProxy) {
      webUi.addProxyTargets(master.id, master.webUiAddress)
    }
    true
  }

  private def removeMaster(master: SiteMasterInfo): Unit = {
    logInfo("Removing site master " + master.id + " on " + master.host + ":" + master.port)
    master.setState(SiteMasterOutState.DEAD)
    idToSiteMaster -= master.id
    idToClusterName -= master.id
    addressToSiteMaster -= master.endpoint.address
    if (reverseProxy) {
      webUi.removeProxyTargets(master.id)
    }
    for (sdriver <- master.siteDrivers.values) {
      logInfo(s"Telling app of lost siteDriver: ${sdriver.id}")
      sdriver.application.driver.send(SiteDriverUpdated(
        sdriver.id, SiteDriverState.LOST, Some("siteMaster lost"), None, siteMasterLost = true
      ))
      sdriver.state = SiteDriverState.LOST
      sdriver.application.removeSiteDriver(sdriver)
    }
    for (gdriver <- master.globalDrivers.values) {
      if (gdriver.desc.supervise) {
        logInfo(s"Re-launching ${gdriver.id}")
        relaunchDriver(gdriver)
      } else {
        logInfo(s"Not re-launching ${gdriver.id} becase it was not supervised")
        removeGlobalDriver(gdriver.id, GlobalDriverState.ERROR, None)
      }
    }
    persistenceEngine.removeSiteMaster(master)
  }

  private def relaunchDriver(driver: GlobalDriverInfo) {
    driver.siteMaster = None
    driver.state = GlobalDriverState.RELAUNCHING
    waitingGlobalDrivers += driver
    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
  ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    // one app one source
    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
    if (reverseProxy) {
      webUi.addProxyTargets(app.id, app.desc.appUiUrl)
    }
  }

  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address
      if (reverseProxy) {
        webUi.removeProxyTargets(app.id)
      }
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (sd <- app.siteDrivers.values) {
        killSiteDriver(sd)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      siteMasters.foreach { sm =>
        sm.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
//  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
//    idToApp.get(appId) match {
//      case Some(appInfo) =>
//        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
//        appInfo.executorLimit = requestedTotal
//        schedule()
//        true
//      case None =>
//        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
//        false
//    }
//  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
//  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
//    idToApp.get(appId) match {
//      case Some(appInfo) =>
//        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
//        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
//        known.foreach { executorId =>
//          val desc = appInfo.executors(executorId)
//          appInfo.removeExecutor(desc)
//          killExecutor(desc)
//        }
//        if (unknown.nonEmpty) {
//          logWarning(s"Application $appId attempted to kill non-existent executors: "
//            + unknown.mkString(", "))
//        }
//        schedule()
//        true
//      case None =>
//        logWarning(s"Unregistered application $appId requested us to kill executors!")
//        false
//    }
//  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  private def killSiteDriver(sd: SiteDriverDesc): Unit = {
    sd.master.removeSiteDriver(sd)
    sd.master.endpoint.send(KillSiteDriver(globalMasterUrl, sd.application.id, sd.id))
    sd.state = SiteDriverState.KILLED
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadMasters() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = siteMasters.filter(
      _.lastHeartbeat < currentTime - SITE_MASTER_TIMEOUT_MS
    ).toArray
    for (master <- toRemove) {
      if (master.state != SiteMasterOutState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          master.id, SITE_MASTER_TIMEOUT_MS / 1000))
        removeMaster(master)
      } else {
        if (master.lastHeartbeat <
          currentTime - ((REAPER_ITERATIONS + 1) * SITE_MASTER_TIMEOUT_MS)) {
          // we've seen this DEAD worker in the UI, etc. for long enough; cull it
          siteMasters -= master
        }
      }
    }
  }

  private def newGlobalDriverId(submitDate: Date): String = {
    val appId = "global-driver-%s-%04d".format(
      createDateFormat.format(submitDate), nextGlobalDriverNumber)
    nextGlobalDriverNumber += 1
    appId
  }

  private def createGlobalDriver(desc: GlobalDriverDescription): GlobalDriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new GlobalDriverInfo(now, newGlobalDriverId(date), desc, date)
  }

  private def launchGlobalDriver(smasterInfo: SiteMasterInfo, gdriverInfo: GlobalDriverInfo) {
    logInfo("Launching global driver " + gdriverInfo.id + " on site master " + smasterInfo.id)
    smasterInfo.addGlobalDriver(gdriverInfo)
    gdriverInfo.siteMaster = Some(smasterInfo)
    smasterInfo.endpoint.send(LaunchGlobalDriver(gdriverInfo.id, gdriverInfo.desc))
    gdriverInfo.state = GlobalDriverState.RUNNING
  }

  private def removeGlobalDriver(
                            driverId: String,
                            finalState: GlobalDriverState,
                            exception: Option[Exception]) {
    globalDrivers.find(gd => gd.id == driverId) match {
      case Some(gdriver) =>
        logInfo(s"Removing global driver: $driverId")
        globalDrivers -= gdriver
        if (completedGlobalDrivers.size >= RETAINED_GLOBAL_DRIVERS) {
          val toRemove = math.max(RETAINED_GLOBAL_DRIVERS / 10, 1)
          completedGlobalDrivers.trimStart(toRemove)
        }
        completedGlobalDrivers += gdriver
        persistenceEngine.removeGlobalDriver(gdriver)
        gdriver.state = finalState
        gdriver.exception = exception
        gdriver.siteMaster.foreach(w => w.removeGlobalDriver(gdriver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown global driver: $driverId")
    }
  }
}

private[deploy] object GlobalMaster extends Logging {
  val SYSTEM_NAME = "sparkGlobalMaster"
  val ENDPOINT_NAME = "GlobalMaster"

  def main(argStrings: Array[String]) {
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new GlobalMasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              webUiPort: Int,
                              conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new GlobalMaster(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
