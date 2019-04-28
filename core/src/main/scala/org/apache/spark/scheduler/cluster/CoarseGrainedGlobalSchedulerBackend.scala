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

import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedGlobalSchedulerBackend._
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils}

private[spark] class CoarseGrainedGlobalSchedulerBackend(
  scheduler: DAGScheduler, val rpcEnv: RpcEnv
) extends GlobalSchedulerBackend with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  protected val totalCoreCount = new AtomicInteger(0)
  // Total number of site drivers that are currently registered
  protected val totalRegisteredSiteDrivers = new AtomicInteger(0)
  protected val conf = scheduler.sc.conf

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  // TODO-lzp: 如何在SiteDriver层面, 用类似的   注册资源/期待资源比, 控制task的提交

  private val createTime = System.currentTimeMillis()

  // Accessing `executorDataMap` in `DriverEndpoint.receive/receiveAndReply` doesn't need any
  // protection. But accessing `executorDataMap` out of `DriverEndpoint.receive/receiveAndReply`
  // must be protected by `CoarseGrainedSchedulerBackend.this`. Besides, `executorDataMap` should
  // only be modified in `DriverEndpoint.receive/receiveAndReply` with protection by
  // `CoarseGrainedSchedulerBackend.this`.
  private val siteDriverDataMap = new mutable.HashMap[String, SiteDriverData]

  private val clusterNameToSDriverId = new mutable.HashMap[String, String]()

  // TODO-lzp: 不确定用id来标识siteDriver会不会有问题, id会不会变, 重启siteDriver会发生什么
  protected val siteDriverReady = new mutable.HashSet[String]  // sitedriverId

  // Number of executors requested by the cluster manager, [[ExecutorAllocationManager]]
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private var requestedTotalExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Site Drivers we have requested the cluster manager to kill that have not died yet; maps
  // the site driver ID to whether it was explicitly killed by the global driver (and thus shouldn't
  // be considered an app-related failure).
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private val siteDriversPendingToRemove = new mutable.HashMap[String, Boolean]

  // TODO-lzp: don't know how
  // The num of current max SiteDriverId used to re-register appMaster
  @volatile protected var currentSiteDriverIdCounter = 0

  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // Site Drivers that have been lost, but for which we don't yet know the real exit reason.
    protected val siteDriversPendingLossReason = new mutable.HashSet[String]

    // If this DriverEndpoint is changed to support multiple threads,
    // then this may need to be changed so that we don't share the serializer
    // instance across threads
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    protected val addressToSiteDriverId = new mutable.HashMap[RpcAddress, String]

    // TODO-lzp: 好像不需要向自己发送ReviveOffers消息
    override def onStart() {}

    override def receive: PartialFunction[Any, Unit] = {
      case ClusterReady(sdriverId, sdriverRef, siteMasterUrl) =>
        if (siteDriverDataMap.contains(sdriverId)) {
          logInfo(s"cluster $sdriverId($sdriverRef) is Ready to receive tasks")
          siteDriverReady += sdriverId
        } else {
          logInfo(s"Unknown cluster $sdriverId($sdriverRef) is report ready")
        }

      // TODO-lzp: 感觉SiteDriver需要周期性或主动性向GlobalDriver汇报各自的task执行进度

      // TODO-lzp: 主动取消某个特定stage的所有task的执行, 如何做

      case LaunchStages(stages: Seq[StageDescription]) =>
        for (stage <- stages) {
          val serializedStage = ser.serialize(stage)
          val sdriverData = siteDriverDataMap(stage.sdriverId)
          logInfo(s"Launching stage ${stage.stageId} on siteDriverId: ${stage.sdriverId} " +
            s"hostname: ${sdriverData.sdriverHost}")
          sdriverData.endpoint.send(LaunchStage(new SerializableBuffer(serializedStage)))
        }

      case SubStageFinished(sdriverId, data) =>
        if (!siteDriverDataMap.contains(sdriverId)) {
          logInfo("the sub stage's result is from unknown siteDriver")
        } else {
          val (results, stageIdx, stageId) = Stage.deserializeStageResult(data.value)
          logInfo(s"received the sub stage's(id: $stageId result from siteDriver($sdriverId)")
          scheduler.subStageFinished(stageId, stageIdx, results)
        }

      case _ =>
        throw new SparkException("not receive anything now")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case RegisterSiteDriver(sdId, sdRef, clusterName, hostname, cores, logUrls) =>
        if (siteDriverDataMap.contains(sdId)) {
          sdRef.send(RegisterSiteDriverFailed(s"Duplicate siteDriver ID: $sdId"))
          context.reply(true)
        } else {
          val sdriverAddress = sdRef.address
          logInfo(s"Registered site driver $sdRef ($sdriverAddress) with ID $sdId")
          addressToSiteDriverId(sdriverAddress) = sdId
          totalRegisteredSiteDrivers.addAndGet(1)
          val data = new SiteDriverData(sdRef, sdriverAddress, clusterName, hostname, logUrls)
          val sdIdPat = """site-driver-(\d+)""".r
          val sdIdPat(sdNum) = sdId
          CoarseGrainedGlobalSchedulerBackend.this.synchronized {
            siteDriverDataMap.put(sdId, data)
            clusterNameToSDriverId.put(clusterName, sdId)
            if (currentSiteDriverIdCounter < sdNum.toInt) {
              currentSiteDriverIdCounter = sdNum.toInt
            }
          }
          sdRef.send(RegisteredSiteDriver)
          // TODO-lzp: 这里其实可以删除的, 如果修改下SiteDriver的注册方式
          context.reply(true)
          listenerBus.post(
            SparkListenerSiteDriverAdded(System.currentTimeMillis(), sdId, data)
          )
        }

      case StopGlobalDriver =>
        context.reply(true)
        stop()

      case StopSiteDrivers =>
        logInfo("Asking each site driver to shut down")
        for ((_, siteDriverData) <- siteDriverDataMap) {
          siteDriverData.endpoint.send(StopSiteDriver)
        }
        context.reply(true)

      case RemoveSiteDriver(sdriverId, reason) =>
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        siteDriverDataMap.get(sdriverId).foreach(_.endpoint.send(StopSiteDriver))
        removeSiteDriver(sdriverId, reason)
        context.reply(true)

      case RetrieveSparkAppConfig =>
        val reply = SparkAppConfig(sparkProperties,
          SparkEnv.get.securityManager.getIOEncryptionKey())
        context.reply(reply)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToSiteDriverId
        .get(remoteAddress)
        .foreach(removeSiteDriver(_, SiteDriverSlaveLost(
          "Remote RPC client disassociated. Likely due to " +
            "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
            "messages.")))
    }

    private def siteDriverIsAlive(executorId: String): Boolean = synchronized {
      !siteDriversPendingToRemove.contains(executorId) &&
        !siteDriversPendingLossReason.contains(executorId)
    }


    // Remove a disconnected site driver from the cluster
    private def removeSiteDriver(sdriverId: String, reason: SiteDriverLossReason): Unit = {
      logDebug(s"Asked to remove executor $sdriverId with reason $reason")
      siteDriverDataMap.get(sdriverId) match {
        case Some(sdriverInfo) =>
          val killed = CoarseGrainedGlobalSchedulerBackend.this.synchronized {
            addressToSiteDriverId -= sdriverInfo.address
            siteDriverDataMap -= sdriverId
            clusterNameToSDriverId -= sdriverInfo.clusterName
            siteDriverReady -= sdriverId
            siteDriversPendingLossReason -= sdriverId
            siteDriversPendingToRemove.remove(sdriverId).getOrElse(false)
          }
          totalRegisteredSiteDrivers.addAndGet(-1)
          scheduler.siteDriverLost(sdriverId, if (killed) SiteDriverKilled else reason)
          listenerBus.post(
            SparkListenerSiteDriverRemoved(System.currentTimeMillis(), sdriverId, reason.toString)
          )
        case None =>
          scheduler.sc.env.blockManager.master.removeSiteDriverAsync(sdriverId)
          logInfo(s"Asked to remove non-existent executor $sdriverId")
      }
    }

    /**
     * Stop making resource offers for the given executor. The executor is marked as lost with
     * the loss reason still pending.
     *
     * @return Whether executor should be disabled
     */
    protected def disableSiteDriver(sdriverId: String): Boolean = {
      val shouldDisable = CoarseGrainedGlobalSchedulerBackend.this.synchronized {
        if (siteDriverIsAlive(sdriverId)) {
          siteDriversPendingLossReason += sdriverId
          true
        } else {
          // Returns true for explicitly killed executors, we also need to get pending loss reasons;
          // For others return false.
          siteDriversPendingToRemove.contains(sdriverId)
        }
      }

      if (shouldDisable) {
        logInfo(s"Disabling site driver $sdriverId.")
        scheduler.siteDriverLost(sdriverId, SiteDriverLossReasonPending)
      }

      shouldDisable
    }

    override def onStop() {}
  }

  var driverEndpoint: RpcEndpointRef = null

  // TaskSchedulerImpl -> StandaloneSchedulerBackend -> this
  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    driverEndpoint = createDriverEndpointRef(properties)
  }

  override def launchStages(stages: Seq[StageDescription]): Unit = {
    driverEndpoint.send(LaunchStages(stages))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  protected def createDriverEndpointRef(
    properties: ArrayBuffer[(String, String)]): RpcEndpointRef = {
    rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties))
  }

  protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(rpcEnv, properties)
  }

  def stopSiteDrivers() {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all site drivers")
        driverEndpoint.askWithRetry[Boolean](StopSiteDrivers)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down site drivers", e)
    }
  }

  override def stop() {
    stopSiteDrivers()
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askWithRetry[Boolean](StopGlobalDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's global driver endpoint", e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure.
   * */
  protected def reset(): Unit = {
    val siteDrivers = synchronized {
      requestedTotalExecutors = 0
      siteDriversPendingToRemove.clear()
      Set() ++ siteDriverDataMap.keys
    }

    // Remove all the lingering executors that should be removed but not yet. The reason might be
    // because (1) disconnected event is not yet received; (2) executors die silently.
    siteDrivers.foreach { eid =>
      removeSiteDriver(eid, SiteDriverSlaveLost(
        "Stale site driver after cluster manager re-registered."))
    }
  }

  // TODO-lzp: 如何定义默认并行度, 还是放到site driver中定义??

  /**
   * Called by subclasses when notified of a lost site master. It just fires the message and
   * returns at once.
   */
  protected def removeSiteDriver(sdriverId: String, reason: SiteDriverLossReason): Unit = {
    // Only log the failure since we don't care about the result.
    driverEndpoint.ask[Boolean](RemoveSiteDriver(sdriverId, reason)).onFailure { case t =>
      logError(t.getMessage, t)
    }(ThreadUtils.sameThread)
  }

  def sufficientResourcesRegistered(): Boolean = true

  // 即所有的siteDriver都已report ready, 则ready
  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered()) {
      logInfo("GlobalSchedulerBackend is ready for scheduling beginning after all site driver" +
        "is registered")
      true
    } else false
  }

  override def hostnameToSDriverId: Map[String, String] = siteDriverDataMap.map{
    case (sdriverId, data) =>
      (data.sdriverHost, sdriverId)
    }.toMap

  override def clusterNameToHostName: Map[String, String] = clusterNameToSDriverId.map {
    case (clusterName, sdriverId) =>
      (clusterName, siteDriverDataMap(sdriverId).sdriverHost)
    }.toMap

//  final def killSiteDrivers(sdriverIds: Seq[String]): Seq[String] =
//    killSiteDrivers(sdriverIds, replace=false, force=false)
//
//  final def killSiteDrivers(
//    sdriverIds: Seq[String], replace: Boolean, force: Boolean): Seq[String] = {
//
//
//  }
}

private[spark] object CoarseGrainedGlobalSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
