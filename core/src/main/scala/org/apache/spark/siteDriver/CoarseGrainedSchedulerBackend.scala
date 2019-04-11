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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Future
import scala.util.{Failure, Success}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.siteDriver.CoarseGrainedSchedulerBackend.ENDPOINT_NAME
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

class CoarseGrainedSchedulerBackend(
  scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv
) extends ExecutorAllocationClient with SiteSchedulerBackend with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  protected val totalCoreCount = new AtomicInteger(0)

  private val siteDriverId: String = scheduler.ssc.siteDriverId

  // Total number of executors that are currently registered
  protected val totalRegisteredExecutors = new AtomicInteger(0)

  protected var conf: SparkConf = scheduler.ssc.conf

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  // 反正是个比例, 无论集群大小都不影响
  private val _minRegisteredRatio = math.min(
    1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  private val maxRegisteredWaitingTimeMs =
  conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")
  private val createTime = System.currentTimeMillis()

  // Accessing `executorDataMap` in `DriverEndpoint.receive/receiveAndReply` doesn't need any
  // protection. But accessing `executorDataMap` out of `DriverEndpoint.receive/receiveAndReply`
  // must be protected by `CoarseGrainedSchedulerBackend.this`. Besides, `executorDataMap` should
  // only be modified in `DriverEndpoint.receive/receiveAndReply` with protection by
  // `CoarseGrainedSchedulerBackend.this`.
  private val executorDataMap = new HashMap[String, ExecutorData] // execId -> ExecutorData

  // Number of executors requested by the cluster manager, [[ExecutorAllocationManager]]
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private var requestedTotalExecutors = 0

  // Number of executors requested from the cluster manager that have not registered yet
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.ssc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure).
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private val executorsPendingToRemove = new HashMap[String, Boolean]

  // A map to store hostname with its possible task number running on it
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var hostToLocalTaskCount: Map[String, Int] = Map.empty

  // The number of pending tasks which is locality required
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var localityAwareTasks = 0

  // The num of current max ExecutorId used to re-register appMaster
  @volatile protected var currentExecutorIdCounter = 0

  // TODO-lzp: register with globaldriver

  class SiteDriverEndpoint(override val rpcEnv: RpcEnv, properties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // Executors that have been lost, but for which we don't yet know the real exit reason.
    protected val executorsPendingLossReason = new HashSet[String] // execId

    // If this DriverEndpoint is changed to support multiple threads,
    // then this may need to be changed so that we don't share the serializer
    // instance across threads
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    private val reviveThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("site-driver-revive-thread")

    override def onStart() {
      // register to global driver
      val gdriverUrl = scheduler.ssc.globalDriverUrl
      val hostname = scheduler.ssc.hostname
      val cores = scheduler.ssc.cores
      logInfo(s"Connecting to global driver: $gdriverUrl")

      // TODO-lzp: 感觉这里可以再复杂些, 来注册global driver
      rpcEnv.asyncSetupEndpointRefByURI(gdriverUrl).flatMap { ref =>
        gdriverEndpoint = Some(ref)
        ref.ask[Boolean](RegisterSiteDriver(
          siteDriverId, self, hostname, cores, extractLogUrls))
      }(ThreadUtils.sameThread).onComplete {
        case Success(msg) =>
        case Failure(e) =>
          exitSiteDriver(
            1, s"Cannot register with global driver: $gdriverUrl", e, false)
      }(ThreadUtils.sameThread)

      // Periodically revive offers to allow delay scheduling to work
      val reviveIntervalMs = conf.getTimeAsMs("spark.siteScheduler.revive.interval", "1s")
      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }

    // TODO-lzp: 这是什么?? 日志文件路径的
    def extractLogUrls: Map[String, String] = {
      val prefix = "SPARK_LOG_URL_"
      sys.env.filterKeys(_.startsWith(prefix))
        .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredSiteDriver =>
        logInfo("Successfully registered with global driver")
        // TODO-lzp: 感觉可以在此处改变一些状态, 比如设置globalDriverUrl等

      case RegisterSiteDriverFailed(msg) =>
        exitSiteDriver(1, s"Site Driver registration failed: $msg")

      case ExecutorStatusUpdate(executorId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              executorInfo.freeCores += scheduler.CPUS_PER_TASK
              makeOffers(executorId) // 这是单独贡献executor
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }

      case LaunchStage(data) =>
        val stageDesc = ser.deserialize[StageDescription](data.value)
        logInfo(s"Got assigned stage ${stageDesc.stageId}")
        scheduler.stageScheduler.stageSubmitted(stageDesc)

      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(KillTask(taskId, executorId, interruptThread))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }

      case ready @ ClusterReady(sdriverId, sdriverRef, smAddress) =>
        if (gdriverEndpoint.isDefined) {
          gdriverEndpoint.get.send(ready)
        }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
        if (executorDataMap.contains(executorId)) {
          executorRef.send(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
          context.reply(true)
        } else {
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.
          val executorAddress = if (executorRef.address != null) {
            executorRef.address
          } else {
            context.senderAddress
          }
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")
          addressToExecutorId(executorAddress) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val data = new ExecutorData(executorRef, executorRef.address, hostname,
            cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (currentExecutorIdCounter < executorId.toInt) {
              currentExecutorIdCounter = executorId.toInt
            }
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          executorRef.send(RegisteredExecutor)
          // Note: some tests expect the reply to come after we put the executor in the map
          context.reply(true)
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))

          makeOffers()
        }

      case StopSiteDriver =>
        context.reply(true)
        stop()

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)

      case RemoveExecutor(executorId, reason) =>
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        // 即Executor可能仍然活着, 这步似乎逻辑上很不确定, 有种多做一步也不会死的感觉.
        executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
        removeExecutor(executorId, reason)
        context.reply(true)

      // 通过消息发送配置
      case RetrieveSparkAppConfig =>
        val reply = SparkAppConfig(properties,
          SparkEnv.get.securityManager.getIOEncryptionKey())
        context.reply(reply)
    }

    // Make fake resource offers on all executors
    private def makeOffers() {
      // Filter out executors under killing
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
      val workOffers = activeExecutors.map { case (id, executorData) => // execId
        WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toIndexedSeq
      // scheduler.resourceOffers返回Seq[Seq[TaskDescription]]
      launchTasks(scheduler.resourceOffers(workOffers))
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId.get(remoteAddress).foreach(
        removeExecutor(_, ExecutorSlaveLost("Remote RPC client disassociated. Likely due to " +
          "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
          "messages.")))
    }

    // Make fake resource offers on just one executor
    private def makeOffers(executorId: String) {
      // Filter out executors under killing
      if (executorIsAlive(executorId)) {
        val executorData = executorDataMap(executorId)
        val workOffers = IndexedSeq(
          WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))
        launchTasks(scheduler.resourceOffers(workOffers))
      }
    }

    private def executorIsAlive(executorId: String): Boolean = synchronized {
      !executorsPendingToRemove.contains(executorId) &&
        !executorsPendingLossReason.contains(executorId)
    }

    // Launch tasks returned by a set of resource offers
    // worker -> core idx -> TaskDescription
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      // 索引并没有特别的意义
      for (task <- tasks.flatten) { // taskDescription
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= maxRpcMessageSize) { // 比最大rpc消息大小还大, 失败
          scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.rpc.message.maxSize (%d bytes). Consider increasing " +
                "spark.rpc.message.maxSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, maxRpcMessageSize)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        } else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK

          logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
            s"${executorData.executorHost}.")

          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster, 主要是更新下变量
    private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      logDebug(s"Asked to remove executor $executorId with reason $reason")
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          // killed表示此Executor是否是被killed掉的
          val killed = CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingLossReason -= executorId
            executorsPendingToRemove.remove(executorId).getOrElse(false)
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          // 告知TaskSchedulerImpl, Executor没了, 或者被driver明确杀死, 或者reason
          scheduler.executorLost(executorId, if (killed) ExecutorKilled else reason)
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
        case None =>
          // SPARK-15262: If an executor is still alive even after the scheduler has removed
          // its metadata, we may receive a heartbeat from that executor and tell its block
          // manager to reregister itself. If that happens, the block manager master will know
          // about the executor, but the scheduler will not. Therefore, we should remove the
          // executor from the block manager when we hit this case.
          scheduler.sparkEnv.blockManager.master.removeExecutorAsync(executorId)
          logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    /**
     * Stop making resource offers for the given executor. The executor is marked as lost with
     * the loss reason still pending.
     * 禁止此executor
     *
     * @return Whether executor should be disabled
     */
    protected def disableExecutor(executorId: String): Boolean = {
      val shouldDisable = CoarseGrainedSchedulerBackend.this.synchronized {
        if (executorIsAlive(executorId)) {
          executorsPendingLossReason += executorId
          true
        } else {
          // Returns true for explicitly killed executors, we also need to get pending loss reasons;
          // For others return false.
          executorsPendingToRemove.contains(executorId)
        }
      }

      if (shouldDisable) {
        logInfo(s"Disabling executor $executorId.")
        scheduler.executorLost(executorId, ExecutorLossReasonPending)
      }

      shouldDisable
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var sdriverEndpoint: RpcEndpointRef = _
  @volatile var gdriverEndpoint: Option[RpcEndpointRef] = None

  protected def minRegisteredRatio: Double = _minRegisteredRatio

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    // TODO-lzp: 还不如直接发送conf, 这里感觉有问题
    sdriverEndpoint = createDriverEndpointRef(properties)
  }

  override def reportStageFinished(data: ByteBuffer): Unit = {
    gdriverEndpoint.foreach(_.send(SubStageFinished(
      siteDriverId,
      new SerializableBuffer(data))))
  }

  protected def createDriverEndpointRef(
    properties: ArrayBuffer[(String, String)]): RpcEndpointRef = {
    rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties))
  }

  protected def createDriverEndpoint(properties: Seq[(String, String)]): SiteDriverEndpoint = {
    new SiteDriverEndpoint(rpcEnv, properties)
  }

  def stopExecutors() {
    try {
      if (sdriverEndpoint != null) {
        logInfo("Shutting down all executors")
        sdriverEndpoint.askWithRetry[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    stopExecutors()
    try {
      if (sdriverEndpoint != null) {
        sdriverEndpoint.askWithRetry[Boolean](StopSiteDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure.
   * */
  protected def reset(): Unit = {
    val executors = synchronized {
      requestedTotalExecutors = 0
      numPendingExecutors = 0
      executorsPendingToRemove.clear()
      Set() ++ executorDataMap.keys
    }

    // Remove all the lingering executors that should be removed but not yet. The reason might be
    // because (1) disconnected event is not yet received; (2) executors die silently.
    executors.foreach { eid =>
      removeExecutor(eid, ExecutorSlaveLost("Stale executor after cluster manager re-registered."))
    }
  }

  // 直接代理给driverEndpoint
  override def reviveOffers() {
    sdriverEndpoint.send(ReviveOffers)
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    sdriverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  /**
   * Called by subclasses when notified of a lost worker. It just fires the message and returns
   * at once.
   * 当被通知失去一个worker时, 由子类调用, 发送消息并立马返回
   */
  protected def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    // Only log the failure since we don't care about the result.
    sdriverEndpoint.ask[Boolean](RemoveExecutor(executorId, reason)).onFailure { case t =>
      logError(t.getMessage, t)
    }(ThreadUtils.sameThread)
  }

  // 会被StandaloneSchedulerBackend覆盖掉
  def sufficientResourcesRegistered: Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) { // 如果有足够的资源被注册
      logInfo("SiteSchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTimeMs) {
      logInfo("SiteSchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeMs(ms)")
      return true
    }
    false
  }

  def reportClusterReady(): Unit = {
    val sdriverId = scheduler.sparkEnv.executorId
    val siteMasterUrl = scheduler.ssc.siteMasterUrl
    logInfo(s"site driver $sdriverId($sdriverEndpoint) in $siteMasterUrl is Ready")
    // TODO-lzp: 增加多次重试
    sdriverEndpoint.send(
      ClusterReady(sdriverId, sdriverEndpoint, siteMasterUrl)
    )
  }

  /**
   * Return the number of executors currently registered with this backend.
   */
  private def numExistingExecutors: Int = executorDataMap.size

  override def getExecutorIds(): Seq[String] = {
    executorDataMap.keySet.toSeq
  }

  /**
   * Request an additional number of executors from the cluster manager.
   * 从集群管理器请求额外的executors
   *
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
          s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")

    val response = synchronized {
      requestedTotalExecutors += numAdditionalExecutors
      numPendingExecutors += numAdditionalExecutors
      logDebug(s"Number of pending executors is now $numPendingExecutors")
      if (requestedTotalExecutors !=
        (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)) {
        logDebug(
          s"""requestExecutors($numAdditionalExecutors): Executor request doesn't match:
             |requestedTotalExecutors  = $requestedTotalExecutors
             |numExistingExecutors     = $numExistingExecutors
             |numPendingExecutors      = $numPendingExecutors
             |executorsPendingToRemove = ${executorsPendingToRemove.size}""".stripMargin)
      }

      // Account for executors pending to be added or removed
      doRequestTotalExecutors(requestedTotalExecutors)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   *
   * 至少在正常模式下, 似乎并不存在动态地请求Executor的事
   *
   * @param numExecutors   The total number of executors we'd like to have. The cluster manager
   *                       shouldn't kill any running executor to reach this number, but,
   *                       if all existing executors were to die, this is the number of executors
   *                       we'd want to be allocated.
   * @param localityAwareTasks   The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  final override def requestTotalExecutors(
    numExecutors: Int,
    localityAwareTasks: Int,
    hostToLocalTaskCount: Map[String, Int]
  ): Boolean = {
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }

    val response = synchronized {
      this.requestedTotalExecutors = numExecutors
      this.localityAwareTasks = localityAwareTasks
      this.hostToLocalTaskCount = hostToLocalTaskCount

      numPendingExecutors =
        math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)

      doRequestTotalExecutors(numExecutors)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return a future whose evaluation indicates whether the request is acknowledged.
   */
  // 用于被覆盖, 真正做请求的具体的事
  protected def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] =
    Future.successful(false)

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final override def killExecutors(executorIds: Seq[String]): Seq[String] = {
    killExecutors(executorIds, replace = false, force = false)
  }

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * When asking the executor to be replaced, the executor loss is considered a failure, and
   * killed tasks that are running on the executor will count towards the failure limits. If no
   * replacement is being requested, then the tasks will not count towards the limit.
   *
   * @param executorIds identifiers of executors to kill
   * @param replace     whether to replace the killed executors with new ones
   * @param force       whether to force kill busy executors
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final def killExecutors(
    executorIds: Seq[String],
    replace: Boolean,
    force: Boolean): Seq[String] = {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")

    val response = synchronized {
      val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
      unknownExecutors.foreach { id =>
        logWarning(s"Executor to kill $id does not exist!")
      }

      // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
      // If this executor is busy, do not kill it unless we are told to force kill it (SPARK-9552)
      val executorsToKill = knownExecutors
        // 此id不在"将删除"列表中
        .filter { id => !executorsPendingToRemove.contains(id) }
        // 强制, 则OK, 如果不强制, 不忙则OK. 非强制的情况下, 要等executor没有task正在执行
        .filter { id => force || !scheduler.isExecutorBusy(id) }
      // 如果替换的, 则此executor不是被杀的
      executorsToKill.foreach { id => executorsPendingToRemove(id) = !replace }

      logInfo(s"Actual list of executor(s) to be killed is ${executorsToKill.mkString(", ")}")

      // If we do not wish to replace the executors we kill, sync the target number of executors
      // with the cluster manager to avoid allocating new ones. When computing the new target,
      // take into account executors that are pending to be added or removed.
      val adjustTotalExecutors =
      if (!replace) {
        requestedTotalExecutors = math.max(requestedTotalExecutors - executorsToKill.size, 0)
        if (requestedTotalExecutors !=
          (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)) {
          logDebug(
            s"""killExecutors($executorIds, $replace, $force): Executor counts do not match:
               |requestedTotalExecutors  = $requestedTotalExecutors
               |numExistingExecutors     = $numExistingExecutors
               |numPendingExecutors      = $numPendingExecutors
               |executorsPendingToRemove = ${executorsPendingToRemove.size}""".stripMargin)
        }
        doRequestTotalExecutors(requestedTotalExecutors)
      } else {
        // 相当于将一些executor重启, 因此在杀掉后, 需要再将同样数目的executor加入pendingExecutor中, 等待启动
        numPendingExecutors += knownExecutors.size
        Future.successful(true)
      }

      val killExecutors: Boolean => Future[Boolean] =
        if (executorsToKill.nonEmpty) {
          _ => doKillExecutors(executorsToKill)
        } else {
          _ => Future.successful(false)
        }

      val killResponse = adjustTotalExecutors.flatMap(killExecutors)(ThreadUtils.sameThread)

      killResponse.flatMap(killSuccessful =>
        Future.successful(if (killSuccessful) executorsToKill else Seq.empty[String])
      )(ThreadUtils.sameThread)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   *
   * @return whether the kill request is acknowledged.
   */
  // 最终其实是通过AppClient向Master发送KillExecutor消息来杀掉的
  protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)

  protected def exitSiteDriver(code: Int,
    reason: String,
    throwable: Throwable = null,
    notifyGlobalDriver: Boolean = true) = {
    val message = "site driver self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }

    if (notifyGlobalDriver && gdriverEndpoint.nonEmpty) {
      gdriverEndpoint.get.ask[Boolean](
        RemoveSiteDriver(siteDriverId, new SiteDriverLossReason(reason))
      ).onFailure { case e =>
        logWarning(s"Unable to notify the global driver due to " + e.getMessage, e)
      }(ThreadUtils.sameThread)
    }

    System.exit(code)
  }
}

object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
