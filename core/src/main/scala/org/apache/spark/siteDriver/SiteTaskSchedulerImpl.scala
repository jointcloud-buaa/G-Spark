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

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a [[LocalSchedulerBackend]] and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 * TaskSchedulerImpl用于调度任务, 对于不同的集群通过一个SchedulerBackend来进行操作. 它也可以在本地进行操作,
 * 通过使用LocalSchedulerBackend并设置isLocal为true. 它只处理通用的逻辑, 像在jobs间决定调度顺序, 唤醒并载入
 * 投机任务等.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 * 客户端应该首先调用#initialize和#start方法, 然后通过#runTasks提交任务集.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class SiteTaskSchedulerImpl(
  val ssc: SiteContext,
  val maxTaskFailures: Int,
  isLocal: Boolean = false)
  extends SiteTaskScheduler with Logging {

  def this(ssc: SiteContext) = this(ssc, ssc.conf.get(config.MAX_TASK_FAILURES))

  val sparkEnv: SparkEnv = ssc.env

  val mapOutputTracker = sparkEnv.mapOutputTracker

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL_MS = ssc.conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  // 即原始任务运行至少这么长时间, 才会载入它的复制副本. 有些任务运行时间特别短, 这样情况下, 就没必要载入投机任务了
  val MIN_TIME_TO_SPECULATION = 100

  // 用于检测投机任务的线程
  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved. 初始任务集被饿死了
  val STARVATION_TIMEOUT_MS = ssc.conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task, 每个task的cpu数  // TODO-lzp: 这货还能是别的??
  val CPUS_PER_TASK = ssc.conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class. 任务集管理器并非线程安全, 所以, 对任何taskSetManager的访问都应该在这个类上同步
  // stageId -> stageAttemptId -> taskSetManager
  // 这点可以理解, taskSetManager并不关心stage或stageAttempt
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  // TODO-lzp: change scheduler to spark
  // taskId  -->  TaskSetManager
  private[spark] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  val taskIdToExecutorId = new HashMap[Long, String] // taskId  -->  execId

  @volatile private var hasReceivedTask = false // 是否有收到任务
  @volatile private var hasLaunchedTask = false // 是否有载入任务
  private val starvationTimer = new Timer(true) // true表示守护进程

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor, 键是executorId, 值是taskId
  // 表示在一个executor上运行了哪些任务 // execId -> Set(taskId)
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  // execId --> task size
  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size) // 任务数
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  // 用来计算存活的主机, 反过来决定在一个给定的主机上我们什么时间获取数据的本地性
  // 这里的host从一开始就是IP地址
  protected val hostToExecutors = new HashMap[String, HashSet[String]] // host -> set(execId)

  protected val hostsByRack = new HashMap[String, HashSet[String]] // rack -> set(host)

  protected val executorIdToHost = new HashMap[String, String] // execId -> host

  var backend: SiteSchedulerBackend = _

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter: TaskResultGetter = _
  // new TaskResultGetter(ssc.env, this)

  def initialize(backend: SiteSchedulerBackend) {
    this.backend = backend
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    // 非本地模式, 且开启了投机执行
    if (!isLocal && ssc.conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryOrStopSiteContext(ssc) {
          // 周期性地执行
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet) {
//    val tasks = taskSet.tasks
//    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
//    this.synchronized { // 同步代码, 即任何时刻只能有一个在submitTasks
//      val manager = createTaskSetManager(taskSet, maxTaskFailures) // 实例化一个TaskSetManager
//    val stage = taskSet.stageId
//      val stageTaskSets =
//        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
//      stageTaskSets(taskSet.stageAttemptId) = manager
//      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
//        ts.taskSet != taskSet && !ts.isZombie
//      }
//      if (conflictingTaskSet) {
//        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
//          s" ${stageTaskSets.toSeq.map {_._2.taskSet.id}.mkString(",")}")
//      }
//      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
//
//      if (!isLocal && !hasReceivedTask) { // 初始为false, 此块肯定执行
//        starvationTimer.scheduleAtFixedRate(new TimerTask() {
//          override def run() {
//            if (!hasLaunchedTask) { // 表示超过一定时间, 竟然还没有载入任务
//              logWarning("Initial job has not accepted any resources; " +
//                "check your cluster UI to ensure that workers are registered " +
//                "and have sufficient resources")
//            } else {
//              this.cancel() // 一旦载入任务, 则取消这个调度
//            }
//          }
//        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
//      }
//      hasReceivedTask = true
//    }
//
//    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  // TODO-lzp: change scheduler to spark
  private[spark] def createTaskSetManager(
    taskSet: TaskSet,
    maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
//    logInfo("Cancelling stage " + stageId)
//    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
//      attempts.foreach { case (_, tsm) =>
//        // There are two possible cases here:
//        // 1. The task set manager has been created and some tasks have been scheduled.
//        //    In this case, send a kill signal to the executors to kill the task and then abort
//        //    the stage.
//        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
//        //    simply abort the stage.
//        tsm.runningTasksSet.foreach { tid =>
//          val execId = taskIdToExecutorId(tid)
//          backend.killTask(tid, execId, interruptThread)
//        }
//        tsm.abort("Stage %s cancelled".format(stageId))
//        logInfo("Stage %d was cancelled".format(stageId))
//      }
//    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  // 是否能在指定的本地性上, 为单个TaskSet在每个offer上载入0/1个task
  // 会修改传入的对象,
  // * availableCpus在每载入一个task就会修改相应offer的可用cpu
  // * tasks在每载入一个task就会添加
  private def resourceOfferSingleTaskSet(
    taskSet: TaskSetManager,
    maxLocality: TaskLocality,
    shuffledOffers: Seq[WorkerOffer],
    availableCpus: Array[Int],
    tasks: IndexedSeq[ArrayBuffer[TaskDescription]]): Boolean = {
    var launchedTask = false
    // 从这个角度看, 是有可能将当前taskSet的多个任务分散到所有的offer的
    // 看是否能在每个offer载入一个task, 一个offer对应一个executor
    for (i <- 0 until shuffledOffers.size) { // 第一层循环, 循环每个资源offer
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK) { // 此executor可载入一个任务
        try {
          // 虽然使用了for, 但是#resourceOffer返回的是一个Option, 也就是说, 是处理一个任务
          // 要说明的是, TaskDescription是在这里实例化的
          // 如果成功调度一个task, 则要处理一些变量, 并修改offer可用的cpu
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   * 由集群管理器调用来提供slaves上的资源. 以轮询的方式用tasks填满每个节点, 来保证tasks被平均分配到
   * 集群上.
   */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
//    var newExecAvail = false
//
//    for (o <- offers) {
//      if (!hostToExecutors.contains(o.host)) {
//        hostToExecutors(o.host) = new HashSet[String]()
//      }
//      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
//        hostToExecutors(o.host) += o.executorId
//        executorAdded(o.executorId, o.host)
//        executorIdToHost(o.executorId) = o.host
//        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
//        newExecAvail = true
//      }
//      for (rack <- getRackForHost(o.host)) {
//        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
//      }
//    }
//
//    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
//    val shuffledOffers = Random.shuffle(offers)
//
//    // Build a list of tasks to assign to each worker.
//    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
//
//    val availableCpus = shuffledOffers.map(o => o.cores).toArray
//
//    val sortedTaskSets = rootPool.getSortedTaskSetQueue // ArrayBuffer[TaskSetManager]
//
//    // 其实是taskSetManager
//    for (taskSet <- sortedTaskSets) { // 这里主要对所有的taskSetManager, 计算下其本地性
//      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
//        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
//      if (newExecAvail) { // 如果有新的executor, 则告诉taskSetManager, 其实是重新计算本地性
//        taskSet.executorAdded() // 每个taskSetManager其实只计算一次
//      }
//    }
//
//    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
//    // of locality levels so that it gets a chance to launch local tasks on all of them.
//    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
//    for (taskSet <- sortedTaskSets) { // 第一层循环, 循环taskSetManager
//      var launchedAnyTask = false // 标识是否载入了任何task
//    var launchedTaskAtCurrentMaxLocality = false // 是否以最大的本地性载入了任务
//      for (currentMaxLocality <- taskSet.myLocalityLevels) { // 第二层循环, 循环本地性级别
//        do {
//          launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
//            taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
//          launchedAnyTask |= launchedTaskAtCurrentMaxLocality
//        } while (launchedTaskAtCurrentMaxLocality) // 若能分配task则继续
//      }
//      if (!launchedAnyTask) {
//        taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
//      }
//    }
//
//    if (tasks.size > 0) {
//      hasLaunchedTask = true
//    }
//    return tasks
    Seq.empty
  }

  // taskId
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
//    var failedExecutor: Option[String] = None
//    var reason: Option[ExecutorLossReason] = None
//    synchronized {
//      try {
//        taskIdToTaskSetManager.get(tid) match {
//          case Some(taskSet) =>
//            if (state == TaskState.LOST) { // 只在弃用的Mesos 细粒度调度模式中使用
//              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
//              // where each executor corresponds to a single task, so mark the executor as failed.
//              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
//                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
//              if (executorIdToRunningTaskIds.contains(execId)) {
//                reason = Some(ExecutorSlaveLost(
//                  s"Task $tid was lost, so marking the executor as lost as well."))
//                removeExecutor(execId, reason.get)
//                failedExecutor = Some(execId)
//              }
//            }
//            if (TaskState.isFinished(state)) { // task结束, 或者成功结束, 或者被杀, 或者失败
//              cleanupTaskState(tid)
//              taskSet.removeRunningTask(tid)
//              if (state == TaskState.FINISHED) { // task成功结束
//                // 异步获取结果
//                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
//             } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
//                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
//              }
//            }
//
//          case None => // 没有此taskSetManager
//            logError(
//              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
//                "likely the result of receiving duplicate task finished status updates) or its " +
//                "executor has been marked as failed.")
//                .format(state, tid))
//        }
//      } catch {
//        case e: Exception => logError("Exception in statusUpdate", e)
//      }
//    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
//    if (failedExecutor.isDefined) { // 在非mesos fine grained模式下, 不用在乎这个
//      assert(reason.isDefined)
//      dagScheduler.executorLost(failedExecutor.get, reason.get)
//      backend.reviveOffers()
//    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   * 更新那些正在执行的任务的测量, 并让Master知道BlockManager是否还活着. 如果driver知道给定的blockManager,
   * 则返回true, 否则返回false, 并指示块管理器重新注册吧.
   */
  override def executorHeartbeatReceived(
    execId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) => // taskId
        // 通过toInfo来获得AccumulatorInfo对象
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    // TODO-lzp: no dag scheduler
//    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
    true
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskState: TaskState,
    reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
//      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  // 因为HeartbeatReceiver, 在未收到心跳, 则执行executorLost
  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) { // 此执行器有正在运行的任务
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
//    if (failedExecutor.isDefined) {
//      dagScheduler.executorLost(failedExecutor.get, reason)
//      backend.reviveOffers()
//    }
  }

  private def logExecutorLoss(
    executorId: String,
    hostPort: String,
    reason: ExecutorLossReason): Unit = reason match {
    case ExecutorLossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    // #remove返回Option
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach {_.remove(tid)}
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId) // 取主机
    val execs = hostToExecutors.getOrElse(host, new HashSet) // 取主机上的所有executors
    execs -= executorId
    if (execs.isEmpty) { // 主机上没有任何的executor
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    if (reason != ExecutorLossReasonPending) { // 如果确切地知道 executor为什么Lost
      executorIdToHost -= executorId
//      rootPool.executorLost(executorId, host, reason)
    }
  }

  def executorAdded(execId: String, host: String) {
//    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  // By default, rack is unknown
  // 在Yarn模式下使用
  def getRackForHost(value: String): Option[String] = None

  // 等待schedulerBackend准备好, 通常, 要么已经注册了足够的资源, 要么等待了足够的时间.
  // 这时, taskSchedulerImpl才能进行资源的调度
  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    // 未准备好则继续等
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (ssc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def siteAppId(): String = backend.siteAppId()

  // TODO-lzp: change scheduler to spark
  private[spark] def taskSetManagerForAttempt(
    stageId: Int,
    stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object SiteTaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T](map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None
}

