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

import java.io.{File, IOException, NotSerializableException}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.{mutable, Map}
import scala.collection.mutable.{HashMap, HashSet, Map => MMap}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.{RDD, RDDCheckpointData}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{AccumulableInfo, ActiveJob, CompressedMapStatus, ExecutorLossReason, ExecutorSlaveLost, HighlyCompressedMapStatus, JobSucceeded, LiveListenerBus, MapStatus, ResultStage, ResultTask, ShuffleMapStage, ShuffleMapTask, SparkListenerExecutorMetricsUpdate, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, Stage, StageDescription, Task, TaskInfo, TaskLocation, TaskSet}
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.{BlockManagerHeartbeat, BlockManagerSiteHeartbeat}
import org.apache.spark.util._

private[spark]
class StageScheduler(
  private[siteDriver] val ssc: SiteContext,
  private[siteDriver] val taskScheduler: TaskScheduler,
  listenerBus: LiveListenerBus,
  mapOutputTracker: MapOutputTrackerMaster,
  blockManagerMaster: BlockManagerMaster,
  env: SparkEnv,
  clock: Clock = new SystemClock())
  extends Logging {

  def this(ssc: SiteContext, taskScheduler: TaskScheduler) = {
    this(
      ssc,
      taskScheduler,
      ssc.listenerBus,
      ssc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      ssc.env.blockManager.master,
      ssc.env)
  }

  def this(ssc: SiteContext) = this(ssc, ssc.taskScheduler)

  private val conf = env.conf

  private[spark] val metricsSource: StageSchedulerSource = new StageSchedulerSource(this)

  // TODO-lzp: StageScheduler/DAGScheduler存储的locs信息不同
  // rddId -> partIndex -> Seq[TaskLocation], 注意, 不是partId, 是partIndex, 是索引
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  private[siteDriver] val stageIdToStage = new HashMap[Int, Stage] // stageId -> Stage

  private[siteDriver] val rddIdToPartitions = new HashMap[Int, Array[Int]]
  // rddId -> partId -> partIndex
  private[siteDriver] val rddPartIdToIndex = new HashMap[Int, Map[Int, Int]]

  // TODO-lzp: 目前还不清楚这有什么用, 记录subStage的Index
  private[siteDriver] val stageIdToIdx = new HashMap[Int, Int] // stageId -> Index
  private[siteDriver] val stageIdToProperties = new HashMap[Int, Properties]
  private[siteDriver] val runningStages = new HashSet[Stage]
  private[siteDriver] val waitingStages = new HashSet[Stage]
  private[siteDriver] val failedStages = new HashSet[Stage]
  private[siteDriver] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]

  private val stageDataAggregateReady: MMap[Int, MMap[Int, (Boolean, Long)]] = MMap.empty

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  // 表示获取的依赖
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private[siteDriver] val outputCommitCoordinator = env.outputCommitCoordinator.get

  // A closure serializer that we reuse.
  // This is only safe because StageScheduler runs in a single thread.
  private val closureSerializer = env.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = ssc.getConf.getBoolean("spark.test.noStageRetry", false)

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.siteDriver.userClassPathFirst", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(urlClassLoader)
  // SPARK-21928.  SerializerManager's internal instance of Kryo might get used in netty threads
  // for fetching remote cached RDD blocks, so need to make sure it uses the right classloader too.
  env.serializerManager.setDefaultClassLoader(urlClassLoader)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")


  private[siteDriver] val eventProcessLoop = new StageSchedulerEventProcessLoop(this)
  taskScheduler.setStageScheduler(this)

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    ssc.userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = ssc.userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  @tailrec
  private def getInboundShuffleDependency(rdd: RDD[_]): Option[ShuffleDependency[_, _, _]] =
    rdd.dependencies.headOption match {
      case None => None
      case Some(dep) => dep match {
        case sdep: ShuffleDependency[_, _, _] => Some(sdep)
        case ndep: NarrowDependency[_] => getInboundShuffleDependency(ndep.rdd)
      }
    }

  /**
   * 建立rdd到其在此集群的分区的映射, 以及在此集群中分区ID到索引的映射
   *
   * @param rdd
   * @param partitions
   */
  def registerRDD(rdd: RDD[_], partitions: Array[Int]): Unit = {
    rddIdToPartitions(rdd.id) = partitions
    rddPartIdToIndex(rdd.id) = partitions.zipWithIndex.toMap
    rdd.dependencies.foreach {
      case ndep: NarrowDependency[_] =>
        registerRDD(ndep.rdd, partitions.flatMap(ndep.getParents))
    }
  }

  def stageSubmitted(stageDesc: StageDescription): Unit = {
    val stageId = stageDesc.stageId
    logInfo(s"stage($stageId) is submitted to StageScheduler")
    val subStageIdx = stageDesc.index
    stageIdToIdx(stageId) = subStageIdx

    Thread.currentThread.setContextClassLoader(urlClassLoader)

    val (stageFiles, stageJars, stageProps, blockMIds, parts, jobId, stageBytes) =
      Stage.deserializeWithDependencies(stageDesc.serializedStage)
    stageIdToProperties(stageId) = stageProps

    updateDependencies(stageFiles, stageJars)

    // 这里才是真正的stage
    val stage = closureSerializer.deserialize[Stage](
      stageBytes, Thread.currentThread.getContextClassLoader)  // 必须指定classLoader

    stage.init(parts)

    registerRDD(stage.rdd, parts)

    stageIdToStage(stage.id) = stage
    // 初始化
    stageDataAggregateReady(stage.id) = MMap.empty

    eventProcessLoop.post(StageSubmitted(jobId, stage))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Seq[AccumulatorV2[_, _]],
    taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
    execId: String,
    // (taskId, stageId, stageAttemptId, accumUpdates)
    accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
    blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerSiteHeartbeat(blockManagerId),
      new RpcTimeout(600 seconds, "BlockManagerHeartbeat")
    )
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  // TODO-lzp: 目前的功能就只是简单的将GD的files/jars拉取到SD上, 这样方便E去获取, 但不完整
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = false)
        currentFiles(name) = timestamp

        // 以下再加入到SiteDriver的file server
        val localName = name.split("/").last
        ssc.addFile(new File(SparkFiles.getRootDirectory(), localName).getPath)
      }
      for ((name, timestamp) <- newJars) {
        val localName = name.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)

          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = false)

          currentJars(name) = timestamp

          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          ssc.addJar(url.toString)
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[siteDriver] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
//        submitStage(stage)
        submitMissingTasks(stage, stage.firstJobId, stageIdToProperties(stage.id))
      }
    }
  }

  private[siteDriver] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[siteDriver] def handleTaskSetFailed(
    taskSet: TaskSet,
    reason: String,
    exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach {abortStage(_, reason, exception)}
  }

  private[siteDriver] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int, properties: Properties) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingPartitions.clear()

    // First figure out the indexes of partition ids to compute.
    // 用来索引Stage的calcPartIds和partResults
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.numSplits - 1)
    }
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      partitionsToCompute.map {id => (id, getPreferredLocs(stage.rdd, stage.calcPartIds(id)))}.toMap
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(ssc, partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(ssc, partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[Partition] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      var taskBinaryBytes: Array[Byte] = null
      // taskBinaryBytes and partitions are both effected by the checkpoint status. We need
      // this synchronization in case another concurrent job is checkpointing this RDD, so we get a
      // consistent view of both variables.
      RDDCheckpointData.synchronized {
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }
        partitions = rddIdToPartitions(stage.rdd.id).map(stage.rdd.partitions)
      }

      taskBinary = ssc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = try {
      // TODO-lzp: 合并一些变量
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, stage.latestInfo.taskMetrics, properties, Option(jobId),
              Option(ssc.siteAppId), ssc.siteAppAttemptId)
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>  // 在集群本地分配到的分区数组中的索引
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics,
              Option(jobId), Option(ssc.siteAppId), ssc.siteAppAttemptId)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingPartitions ++= tasks.map(_.partitionId)
      logDebug("New pending partitions: " + stage.pendingPartitions)
      taskScheduler.submitTasks(new TaskSet(
        // TODO-lzp: 原则上此处的jobId用来决定Stage执行的优先级, 没有用, 但还是留着
        // 万一以后需要在SiteDriver上同时调度多个Stage
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage: ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)

      submitWaitingStages(stage)
    }
  }

  // TODO-lzp: 如果SiteDriver有多个无依赖的Stage需要调度, 可能在此处
  private def submitWaitingStages(stage: Stage): Unit = {

  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.accumulables += acc.toInfo(Some(updates.value), Some(acc.value))
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  private[siteDriver]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      val parts = rddIdToPartitions(rdd.id)
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(parts.length)(Nil)  // 用Nil填充
      } else {
        val blockIds = parts.map(partId => RDDBlockId(rdd.id, partId)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }


  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[siteDriver] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.stageAttemptId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    listenerBus.post(SparkListenerTaskEnd(
      stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        stage.pendingPartitions -= task.partitionId
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            updateAccumulators(event)
            // outputId用来索引calcPartIds和partResults
            resultStage.addPartResult(rt.outputId, event.result)
            if (resultStage.isSiteAvailble()) {
              val result = Stage.serializeStageResult(
                resultStage.id,
                resultStage.calcPartIds,
                resultStage.getPartResults
              )
              taskScheduler.asInstanceOf[TaskSchedulerImpl].backend
                .reportStageFinished(result)
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addPartResult(smt.outputId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.calcPartIds,
                shuffleStage.getPartResults,
                // TODO-lzp: 不是很确定是否要更改
                changeEpoch = true)

              // 报告结果, 向mapOutput注册想了想还是在GD中进行吧
              val bmId = env.blockManager.blockManagerId
              // 这里进行了替换, 这样在GD看来, 此集群使用同一个blockManagerId
              val newMapStatuses = shuffleStage.getPartResults.map { ms =>
                Option(ms).map(_.replaceLoc(bmId)).getOrElse(null)
              }
              val result = Stage.serializeStageResult(
                shuffleStage.id,
                shuffleStage.calcPartIds,
                newMapStatuses
              )
              taskScheduler.asInstanceOf[TaskSchedulerImpl].backend
                .reportStageFinished(result)

              clearCacheLocs()

              if (!shuffleStage.isSiteAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                // TODO-lzp: 这里需要更具体的情况, 为什么会存在要计算的分区都成功了, 但是stage块不成功
                // 虽然我觉得, 这不是SiteDriver要处理的问题
//                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                // TODO-lzp: 这里是处理ShuffleMapStageJob
//                if (shuffleStage.mapStageJobs.nonEmpty) {
//                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
//                  for (job <- shuffleStage.mapStageJobs) {
//                    markMapStageJobAsFinished(job, stats)
//                  }
//                }
                submitWaitingStages(shuffleStage)
              }
            }
        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingPartitions += task.partitionId

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          val shouldAbortStage =
            failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId) ||
              disallowStageRetryForTest

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, errorMessage = Some(failureMessage),
              willRetry = !shouldAbortStage)
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId)) {
            abortStage(failedStage, s"$failedStage (${failedStage.name}) " +
              s"has failed the maximum allowable number of " +
              s"times: ${Stage.MAX_CONSECUTIVE_FETCH_FAILURES}. " +
              s"Most recent failure reason: ${failureMessage}", None)
          } else {
            if (failedStages.isEmpty) {
              // Don't schedule an event to resubmit failed stages if failed isn't empty, because
              // in that case the event will already have been scheduled.
              // TODO: Cancel running tasks in the stage
              logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, StageScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
            failedStages += failedStage
            failedStages += mapStage
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
      // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Tasks failed with exceptions might still have accumulator updates.
        updateAccumulators(event)

      case TaskResultLost =>
      // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | TaskKilled | UnknownReason =>
      // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
      // will abort the job.
    }
  }

  // TODO-lzp: 应该删除, 是为了屏蔽handleTaskCompletion的错误
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {

  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
   * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
   * presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[siteDriver] def handleExecutorLost(
    execId: String,
    filesLost: Boolean,
    maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleIdToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.calcPartIds,
            stage.getPartResults,
            changeEpoch = true
          )
        }
        if (shuffleIdToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
        "(epoch " + currentEpoch + ")")
    }
  }

  private[siteDriver] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[siteDriver] def handleStageCancellation(stageId: Int): Unit = {
    failStage(stageId, s"because Stage $stageId was cancelled")
  }

  private[siteDriver] def handleStageSubmitted(jobId: Int, stage: Stage): Unit = {
    // 注册下Shuffle
    stage match {
      case sms: ShuffleMapStage =>
        val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
        val dep = shuffleStage.shuffleDep
        if (mapOutputTracker.containsShuffle(dep.shuffleId)) {
          val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(dep.shuffleId)
          val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
          locs.indices.foreach { idx =>
            if (locs(idx) ne null) {
              shuffleStage.addPartResult(idx, locs(idx))
            }
          }
        } else {
          mapOutputTracker.registerShuffle(dep.shuffleId, stage.calcPartIds.length)
        }
      case _ =>
    }

    submitMissingTasks(stage, jobId, stageIdToProperties(stage.id))
  }

  // TODO-lzp: 取消在此SiteDriver执行的所有Stages
  private[siteDriver] def doCancelAllStages(): Unit = {

  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(
    stage: Stage,
    errorMessage: Option[String] = None,
    willRetry: Boolean = false): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    if (!willRetry) {
      outputCommitCoordinator.stageEnd(stage.id)
    }
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[siteDriver] def abortStage(
    failedStage: Stage,
    reason: String,
    exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    failStage(failedStage.id, reason, exception)
  }

  private def failStage(
    stageId: Int, failureReason: String, exception: Option[Throwable] = None): Unit = {
    val properties = stageIdToProperties(stageId)

    val shouldInterruptThread = if (properties == null) false
    else properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean
    var ableToCancelStage = true
    if (!stageIdToStage.contains(stageId)) {
      logError(s"Missing Stage for stage with id $stageId")
    } else {
      val stage = stageIdToStage(stageId)
      if (runningStages.contains(stage)) {
        try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
          // 取消任务集
          taskScheduler.cancelTasks(stageId, shouldInterruptThread)
          markStageAsFinished(stage, Some(failureReason))
        } catch {
          case e: UnsupportedOperationException =>
            logInfo(s"Could not cancel tasks for stage $stageId", e)
            ableToCancelStage = false
        }
      }
    }

    if (ableToCancelStage) {
      // TODO-lzp: 向GD发消息, 表示此Stage已经取消, 附带原因
    }
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both StageScheduler and SparkContext.
   *
   * @param rdd       whose partitions are to be looked at
   * @param part to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  // idx表示求第几个分区的位置偏好， 因为此集群只得到了一部分的分区， 因此不能以分区ID来计算
  // TODO-lzp: 这里有问题， 只是临时这么写, 相当于假设了每个RDD只有一个父分区
  private[spark] def getPreferredLocs(rdd: RDD[_], part: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, part, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses StageScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * StageScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
    rdd: RDD[_],
    part: Int,
    visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, part))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }

    // If the partition is cached, return the cache locations
    val partIndex = rddPartIdToIndex(rdd.id)(part)
    val cached = getCacheLocs(rdd)(partIndex)
    if (cached.nonEmpty) {
      return cached
    }

    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(part)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        // TODO-lzp: 原则上, 这里还可以实现的更好一点, 子分区有多个父分区, 如果将多个父分区的位置信息聚合
        for (inPart <- n.getParents(part)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  private[siteDriver] def cleanUpAfterSchedulerStop(): Unit = {
    val error =
      new SparkException(s"Stage cancelled because SparkContext was shut down")
    val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
    runningStages.toArray.foreach { stage =>
      markStageAsFinished(stage, Some(stageFailedMessage))
    }
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

private[siteDriver] class StageSchedulerEventProcessLoop(stageScheduler: StageScheduler)
  extends EventLoop[StageSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = stageScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: StageSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: StageSchedulerEvent): Unit = event match {
    case StageCancelled(stageId) =>
      stageScheduler.handleStageCancellation(stageId)

    case StageSubmitted(jobId, stage) =>
      stageScheduler.handleStageSubmitted(jobId, stage)

    case ExecutorAdded(execId, host) =>
      stageScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case ExecutorSlaveLost(_, true) => true
        case _ => false
      }
      stageScheduler.handleExecutorLost(execId, filesLost)

    case BeginEvent(task, taskInfo) =>
      stageScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      stageScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      stageScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      stageScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      stageScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("StageSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      stageScheduler.doCancelAllStages()
    } catch {
      case t: Throwable => logError("StageScheduler failed to cancel all jobs.", t)
    }
    stageScheduler.ssc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    stageScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object StageScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
