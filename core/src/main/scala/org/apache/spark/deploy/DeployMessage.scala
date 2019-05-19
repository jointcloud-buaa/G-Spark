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

package org.apache.spark.deploy

import scala.collection.immutable.List

import org.apache.spark.deploy.ExecutorState.ExecutorState
import org.apache.spark.deploy.globalmaster.{ApplicationInfo, GlobalDriverInfo, SiteMasterInfo}
import org.apache.spark.deploy.globalmaster.GlobalDriverState.GlobalDriverState
import org.apache.spark.deploy.globalmaster.GlobalMasterState.GlobalMasterState
import org.apache.spark.deploy.globalmaster.NetworkMetricDaemonState.NetworkMetricDaemonState
import org.apache.spark.deploy.globalmaster.SiteDriverState.SiteDriverState
import org.apache.spark.deploy.sitemaster._
import org.apache.spark.deploy.sitemaster.SiteMasterInState.SiteMasterInState
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[deploy] sealed trait DeployMessage extends Serializable

/** Contains messages sent between Scheduler endpoint nodes. */
private[deploy] object DeployMessages {

  // siteMaster to globalMaster

  case class RegisterSiteMaster(
      id: String,
      host: String,
      port: Int,
    clusterName: String,
      master: RpcEndpointRef,
      cores: Int,
      memory: Int,
      smWebUiUrl: String)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert(port > 0)
  }

  // Worker to Site Master

  case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      worker: RpcEndpointRef,
      cores: Int,
      memory: Int,
      workerWebUiUrl: String)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert (port > 0)
  }

  case class ExecutorStateChanged(
      appId: String,
      execId: Int,
      state: ExecutorState,
      message: Option[String],
      exitStatus: Option[Int])
    extends DeployMessage

  case class GlobalDriverStateChanged(
    gdriverId: String,
    state: GlobalDriverState,
    exception: Option[Exception]) extends DeployMessage

  case class SiteDriverStateChanged(
    appId: String,
    sdriverId: String,
    state: SiteDriverState,
    message: Option[String],
    exitStatus: Option[Int]) extends DeployMessage

  case class NetworkMetricDaemonStateChanged(
    siteMasterId: String,
    state: NetworkMetricDaemonState,
    msg: Option[String],
    exitStatus: Option[Int]) extends DeployMessage

  case class WorkerSchedulerStateResponse(id: String, executors: List[ExecutorDescription])

  case class SiteMasterSchedulerStateResponse(
    smId: String,
    sdrivers: List[SiteDriverDescription],
    gdriverIds: Seq[String])

  /**
   * A worker will send this message to the master when it registers with the master. Then the
   * master will compare them with the executors and drivers in the master and tell the worker to
   * kill the unknown executors and drivers.
   */
  case class WorkerLatestState(
      id: String,
      executors: Seq[ExecutorDescription]) extends DeployMessage

  case class SiteMasterLatestState(
                                  smId: String,
                                  siteDrivers: Seq[SiteDriverDescription],
                                  gdriverIds: Seq[String]) extends DeployMessage

  case class SiteMasterHeartbeat(smId: String, smRef: RpcEndpointRef) extends DeployMessage

  case class WorkerHeartbeat(workerId: String, workerRef: RpcEndpointRef) extends DeployMessage

  // global master to site master

  sealed trait RegisterSiteMasterResponse

  case class RegisteredSiteMaster(globalMaster: RpcEndpointRef, globalMasterWebUiUrl: String)
    extends DeployMessage with RegisterSiteMasterResponse

  case class RegisterSiteMasterFailed(message: String)
    extends DeployMessage with RegisterSiteMasterResponse

  case object GlobalMasterInStandby extends DeployMessage with RegisterSiteMasterResponse

  case class ReconnectSiteMaster(gmUrl: String) extends DeployMessage

  case class KillSiteDriver(gmUrl: String, appId: String, sdId: String) extends DeployMessage

  case class LaunchSiteDriver(
    gmasterUrl: String,
    appId: String,
    sdriverId: String,
    appDesc: ApplicationDescription,
    cores: Int,
    memory: Int)
    extends DeployMessage

  case class LaunchNetworkMetricDaemon(gmUrl: String) extends DeployMessage

  case class LaunchGlobalDriver(driverId: String, driverDesc: GlobalDriverDescription)
    extends DeployMessage

  case class KillGlobalDriver(gdriverId: String) extends DeployMessage

  // Site Master to Worker

  sealed trait RegisterWorkerResponse

  case class RegisteredWorker(master: RpcEndpointRef, masterWebUiUrl: String) extends DeployMessage
    with RegisterWorkerResponse

  case class RegisterWorkerFailed(message: String) extends DeployMessage with RegisterWorkerResponse

  case object SiteMasterInStandby extends DeployMessage with RegisterWorkerResponse

  case class ReconnectWorker(masterUrl: String) extends DeployMessage

  case class KillExecutor(masterUrl: String, siteAppId: String, execId: Int) extends DeployMessage

  case class LaunchExecutor(
      smasterUrl: String,
      siteAppId: String,
      execId: Int,
      siteAppDesc: SiteAppDescription,
      cores: Int,
      memory: Int)
    extends DeployMessage

  case class ApplicationFinished(id: String)

  case class SiteAppFinished(id: String)

  // Worker internal

  case object WorkDirCleanup // Sent to Worker endpoint periodically for cleaning up app folders

  case object ReregisterWithSiteMaster // used when a worker attempts to reconnect to a master

  case object ReregisterWithGlobalMaster

  // AppClient to Master

  case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef)
    extends DeployMessage

  case class RegisterSiteApplication(
    siteAppDescription: SiteAppDescription,
    siteDriver: RpcEndpointRef) extends DeployMessage

  case class UnregisterApplication(appId: String)

  case class GlobalMasterChangeAcknowledged(appId: String)

  case class SiteMasterChangeAcknowledged(siteAppId: String)

  case class RequestExecutors(appId: String, requestedTotal: Int)

  case class KillExecutors(appId: String, executorIds: Seq[String])

  // Master to AppClient

  case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends DeployMessage

  case class RegisteredSiteApplication(
    siteAppId: String,
    smaster: RpcEndpointRef) extends DeployMessage

  // TODO(matei): replace hostPort with host
  case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
    Utils.checkHostPort(hostPort, "Required hostport")
  }

  case class SiteDriverAdded(id: String, smasterId: String, hostPort: String,
                             cores: Int, memory: Int) {
    Utils.checkHostPort(hostPort, "Required hostport")
  }

  case class SiteDriverUpdated(
    id: String,
    state: SiteDriverState,
    message: Option[String],
    exitStatus: Option[Int],
    siteMasterLost: Boolean  // 是否SiteMaster丢失
  )

  case class ExecutorUpdated(
    id: Int,
    state: ExecutorState,
    message: Option[String],
    exitStatus: Option[Int],
    workerLost: Boolean
  )

  case class ApplicationRemoved(message: String)

  case class SiteAppRemoved(message: String)

  // DriverClient <-> Master

  case class RequestSubmitGlobalDriver(
    driverDescription: GlobalDriverDescription) extends DeployMessage

  case class SubmitGlobalDriverResponse(
                                   gmasterRef: RpcEndpointRef,
                                   success: Boolean,
                                   gdriverId: Option[String],
                                   message: String) extends DeployMessage

  case class SubmitSiteDriverResponse(
      master: RpcEndpointRef, success: Boolean, driverId: Option[String], message: String)
    extends DeployMessage

  case class RequestKillDriver(driverId: String) extends DeployMessage

  case class KillGlobalDriverResponse(
                                 master: RpcEndpointRef,
                                 driverId: String,
                                 success: Boolean,
                                 message: String) extends DeployMessage

  case class KillSiteDriverResponse(
      master: RpcEndpointRef, driverId: String, success: Boolean, message: String)
    extends DeployMessage

  case class RequestDriverStatus(driverId: String) extends DeployMessage

  case class GlobalDriverStatusResponse(found: Boolean, state: Option[GlobalDriverState],
                                        siteMasterId: Option[String],
                                        siteMasterHostPort: Option[String],
                                        exception: Option[Exception])

  case class SiteDriverStatusResponse(found: Boolean, state: Option[SiteDriverState],
                                      workerId: Option[String],
                                      workerHostPort: Option[String],
                                      exception: Option[Exception])
  // Internal message in AppClient

  case object StopAppClient

  case object StopSiteAppClient

  case class GetSiteDriverIdsForApp(appId: String)

  case class SiteDriverIdsForAppResponse(appId: String, sdriverIds: Array[String])

  case object GetSiteDriverIds

  case class GetNetworkMetricDataForApp(appId: String)

  case class NetworkMetricDataForAppResponse(
    appId: String, data: Map[String, Map[String, (Long, Double)]])

  case object GetNetworkMetricData

  // Master to Worker & AppClient

  case class SiteMasterChanged(master: RpcEndpointRef, masterWebUiUrl: String)

  case class GlobalMasterChanged(master: RpcEndpointRef, masterWebUiUrl: String)

  // MasterWebUI To Master

  case object RequestMasterState

  // Master to MasterWebUI

  case class GlobalMasterStateResponse(
    host: String,
    port: Int,
    restPort: Option[Int],
    siteMasters: Array[SiteMasterInfo],
    activeApps: Array[ApplicationInfo],
    completedApps: Array[ApplicationInfo],
    activeDrivers: Array[GlobalDriverInfo],
    completedDrivers: Array[GlobalDriverInfo],
    status: GlobalMasterState) {

    Utils.checkHost(host, "Required hostname")
    assert (port > 0)

    def uri: String = "spark://" + host + ":" + port
    def restUri: Option[String] = restPort.map { p => "spark://" + host + ":" + p }
  }

  case class SiteMasterStateResponse(
    host: String,
    port: Int,
    smId: String,
    gmUrl: String,
    gmWebUiUrl: String,
    cores: Int,
    memory: Int,
    coresUsed: Int,
    memoryUsed: Int,
    workers: Array[WorkerInfo],
    activeSiteApps: Array[SiteAppInfo],
    completedSiteApps: Array[SiteAppInfo],
    activeSiteDrivers: List[SiteDriverRunner],
    finishedSiteDrivers: List[SiteDriverRunner],
    activeGlobalDrivers: List[GlobalDriverRunner],
    finishedGlobalDrivers: List[GlobalDriverRunner],
    state: SiteMasterInState)

  //  WorkerWebUI to Worker

  case object RequestWorkerState

  case object RequestSiteMasterState

  // Worker to WorkerWebUI

  case class WorkerStateResponse(
                                  host: String,
                                  port: Int,
                                  workerId: String,
                                  executors: List[ExecutorRunner],
                                  finishedExecutors: List[ExecutorRunner],
                                  masterUrl: String,
                                  cores: Int,
                                  memory: Int,
                                  coresUsed: Int,
                                  memoryUsed: Int,
                                  masterWebUiUrl: String) {

    Utils.checkHost(host, "Required hostname")
    assert (port > 0)
  }

  // Liveness checks in various places

  case object SendHeartbeat

}
