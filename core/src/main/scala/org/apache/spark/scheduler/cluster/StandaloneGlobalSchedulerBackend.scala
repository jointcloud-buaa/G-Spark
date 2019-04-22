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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.HashSet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.{DAGScheduler, SiteDriverExited, SiteDriverLossReason, SiteDriverSlaveLost}
import org.apache.spark.util.Utils

private[spark] class StandaloneGlobalSchedulerBackend(
  scheduler: DAGScheduler,
  sc: SparkContext,
  masters: Array[String]
) extends CoarseGrainedGlobalSchedulerBackend(scheduler, sc.env.rpcEnv)
  with StandaloneAppClientListener
  with Logging {

  private var client: StandaloneAppClient = _
  private val stopping = new AtomicBoolean(false)
  // connect with Launcher Server
  private val launcherBackend = new LauncherBackend() {
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneGlobalSchedulerBackend => Unit = _
  @volatile private var appId: String = _

  // 设置一个初始为0的信号量
  private val registrationBarrier = new Semaphore(0)

  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)

  override def start(): Unit = {
    super.start()

    if (sc.deployMode == "client") {
      launcherBackend.connect()
    }

    // The endpoint for site drivers to talk to us
    val gdriverUrl = RpcEndpointAddress(
      sc.conf.get("spark.globalDriver.host"),
      sc.conf.get("spark.globalDriver.port").toInt,
      CoarseGrainedGlobalSchedulerBackend.ENDPOINT_NAME).toString
    val args = Seq(
      "--global-driver-url", gdriverUrl,
      "--site-driver-id", "{{SITE_DRIVER_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--site-master-url", "{{SITE_MASTER_URL}}",
      "--cluster-name", "{{CLUSTER_NAME}}"
    )
    val extraJavaOpts = sc.conf.getOption("spark.siteDriver.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.getOption("spark.siteDriver.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.getOption("spark.siteDriver.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath = if (sys.props.contains("spark.testing")) {
      sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
    } else {
      Nil
    }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isComponetStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = Command("org.apache.spark.siteDriver.SiteDriverWrapper",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    val coresPerSiteDriver = conf.getOption("spark.siteDriver.cores").map(_.toInt)
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    val initialExecutorLimit = if (Utils.isDynamicAllocationEnabled(conf)) {
      Some(0)
    } else {
      None
    }
    val appDesc = ApplicationDescription(
      sc.appName,
      maxCores,
      sc.siteDriverMemory,
      sc.executorMemory,
      command,
      appUIAddress,
      sc.eventLogDir,
      sc.eventLogCodec,
      coresPerSiteDriver,
      coresPerExecutor
    )
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = stop(SparkAppHandle.State.FINISHED)

  // 以下方法属于ClientListener, 由AppClient调用
  override def connected(appId: String): Unit = {
    logInfo(s"Connected to Spark Cluster with app Id: $appId")
    this.appId = appId
    notifyContext()
    launcherBackend.setAppId(appId)
  }

  // application register failed or ClientEndpoint disconnect
  override def disconnected(): Unit = {
    notifyContext()
    if (!stopping.get) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String): Unit = {
    notifyContext()
    if (!stopping.get) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def siteDriverAdded(fullId: String, smId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted site driver ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def siteDriverRemoved(
    fullId: String, message: String, exitStatus: Option[Int], siteMasterLost: Boolean): Unit = {
    val reason: SiteDriverLossReason = exitStatus match {
      case Some(code) => SiteDriverExited(code, exitCausedByApp = true, message)
      case None => SiteDriverSlaveLost(message, siteMasterLost = siteMasterLost)
    }
    logInfo(s"SiteDriver $fullId removed: $message")
    removeSiteDriver(fullId.split("/")(1), reason)
  }

  def notifyContext(): Unit = registrationBarrier.release()

  def waitForRegistration(): Unit = registrationBarrier.acquire()

  private def stop(finalState: SparkAppHandle.State): Unit = {
    if (stopping.compareAndSet(false, true)) {
      try {
        super.stop()  // stop SchedulerBackend
        client.stop()  // stop AppClient

        val callback = shutdownCallback
        if (callback != null) {
          callback(this)
        }
      } finally {
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }

  // 确保siteDriverReady的值, 包含所有SiteMasterUrl
  override def sufficientResourcesRegistered(): Boolean = {
    val sdriverIds = client.getSiteDriverIds()
    (sdriverIds.toSet -- siteDriverReady).isEmpty
  }

  override def applicationId(): String = Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  // TODO-lzp: 能否将requestTotalExecutors, 分别给不同集群去请求, 但目的又是什么呢
}
