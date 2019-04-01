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

package org.apache.spark.scheduler

import org.apache.spark.{SparkContext, SparkException, TaskFailedReason}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

private[spark] class GlobalTaskSchedulerImpl(
  val sc: SparkContext,
  isLocal: Boolean = false
) extends GlobalTaskScheduler with Logging {

  val conf = sc.conf

  var backend: GlobalSchedulerBackend = null
  var dagScheduler: DAGScheduler = null

  var rootPool: Pool = null
  var schedulableBuilder: SchedulableBuilder = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {  // 调度模式
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: GlobalSchedulerBackend): Unit = {
    this.backend = backend
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported spark.scheduler.mode: $schedulingMode")
      }
    }
    schedulableBuilder.buildPools()
  }

  override def start(): Unit = {
    backend.start()

    // TODO-lzp: 在两层架构下, 投机执行是什么概念, 此处是否需要checkSpeculatableTasks
    // 还是说, 将这个概念下放到SiteDriver
  }

  override def stop(): Unit = {
    if (backend != null) backend.stop()
    // TODO-lzp: starvationTimer?? taskResultGetter??
  }

  override def postStartHook(): Unit = waitBackendReady()

  // TODO-lzp: 提交任务意味着什么
  override def submitTasks(taskSet: TaskSet): Unit = {}

  // TODO-lzp: 取消任务又意味着什么
  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {}

  // TODO-lzp: site driver lost会影响什么
  override def siteDriverLost(sdriverId: String, reason: SiteDriverLossReason): Unit = {}

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  // TODO-lzp: 是要收集所有集群的所有执行器的核心数, 还是怎么做
  override def defaultParallelism(): Int = 3

  // TODO-lzp: 要建立的是taskSubSetId, 即每个任务子集一个Id, 这是固定的, 就是名字有点长

  override def siteDriverHeartbeatReceived(
    sdriverId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId): Boolean = {
    // TODO-lzp: 如何提取累积量, 更大的问题是, 这个累积量到底跟什么相关, 在site driver中
//    // (taskSubSetId, stageId, stageAttemptId, accumUpdates)
//    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
//      accumUpdates.flatMap { case (id, updates) =>  // taskSubSetId
//        // 通过toInfo来获得AccumulatorInfo对象
//        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
//        taskSubSetIdToTaskSetManager.get(id).map { taskSetMgr =>
//          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
//        }
//      }
//    }
    val emptyRst = Array.empty[(Long, Int, Int, Seq[AccumulableInfo])]
    dagScheduler.executorHeartbeatReceived(sdriverId, emptyRst, blockManagerId)
  }

  // TODO-lzp: from scheduler backend, to stop all stage
  def error(msg: String): Unit = {

  }

  // 等待schedulerBackend准备好, 通常, 要么已经注册了足够的资源, 要么等待了足够的时间.
  // 这时, taskSchedulerImpl才能进行资源的调度
  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    // 未准备好则继续等
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  private def logSiteDriverLoss(
    sdriverId: String,
    hostPort: String,
    reason: SiteDriverLossReason): Unit = reason match {
    case SiteDriverLossReasonPending =>
      logDebug(s"SiteDriver $sdriverId on $hostPort lost, but reason not yet known.")
    case SiteDriverKilled =>
      logInfo(s"SiteDriver $sdriverId on $hostPort killed by global driver.")
    case _ =>
      logError(s"Lost site driver $sdriverId on $hostPort: $reason")
  }

  // TODO-lzp: 可能要调整分配给此siteDriver的任务, 但是, 如果此SiteDriver丢失, 其实我们什么也做不了
  // 因为, 我们没办法获取此SiteDriver中的数据
  private def removeSiteDriver(sdriverId: String, reason: SiteDriverLossReason): Unit = {

  }

  def siteDriverAdded(sdriverId: String, host: String) {
    dagScheduler.siteDriverAdded(sdriverId, host)
  }

  // TODO-lzp: 目前还没想清楚, 如何表达任务分发
  def runningTasksBySiteDrivers: Map[String, Int] = synchronized {
    Map.empty[String, Int]
  }

  // TODO-lzp: 对任务的处理还未想明白
  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
//    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskResult: DirectTaskResult[_]): Unit = synchronized {
//    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskState: TaskState,
    reason: TaskFailedReason): Unit = synchronized {
//    taskSetManager.handleFailedTask(tid, taskState, reason)
//    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
//      // Need to revive offers again now that the task set manager state has been updated to
//      // reflect failed tasks that need to be re-run.
//      backend.reviveOffers()
//    }
  }
}
