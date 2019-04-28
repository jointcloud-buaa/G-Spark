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

import java.io.{DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, CallSite, Utils}

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 */
// TODO-lzp: 这里改动了下访问范围, 从scheduler改为spark, 为了便于siteDriver访问
private[spark] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    @transient val parents: List[Stage],
    @transient val firstJobId: Int,
    val callSite: CallSite)
  extends Serializable with Logging {

  val numPartitions: Int = rdd.partitions.length

  // 注意: 对于ShuffleMapStage而言, GD/SD的numFinished有不同的含义
  @transient var numFinished: Int = 0

  /** Set of jobs that this stage belongs to. */
  @transient val jobIds = new HashSet[Int]

  val pendingPartitions = new HashSet[Int]

  def init(parts: Array[Int]): Unit = {
    _calcPartIds = parts
    partResults = Array.fill(parts.length)(Nil)
  }

  protected var partResults: Array[List[Any]] = _

  def addPartResult(idx: Int, result: Any): Unit = {
    val prevList = partResults(idx)
    partResults(idx) = result :: prevList
    if (prevList == Nil) {
      numFinished += 1
    }
  }

  def getPartResults: Array[Any] = {
    partResults.map(_.headOption.orNull)
  }

  def isSiteAvailable: Boolean = numFinished == _calcPartIds.length

  private var _calcPartIds: Array[Int] = _

  def calcPartIds: Array[Int] = _calcPartIds

  /** The ID to use for the next new attempt for this stage. */
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [StageInfo] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */
  @transient private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /**
   * Set of stage attempt IDs that have failed with a FetchFailure. We keep track of these
   * failures in order to avoid endless retries if a stage keeps failing with a FetchFailure.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  private val fetchFailedAttemptIds = new HashSet[Int]

  private[spark] def clearFailures() : Unit = {
    fetchFailedAttemptIds.clear()
  }

  /**
   * Check whether we should abort the failedStage due to multiple consecutive fetch failures.
   *
   * This method updates the running set of failed stage attempts and returns
   * true if the number of failures exceeds the allowable number of failures.
   */
  private[spark] def failedOnFetchAndShouldAbort(stageAttemptId: Int): Boolean = {
    fetchFailedAttemptIds.add(stageAttemptId)
    fetchFailedAttemptIds.size >= Stage.MAX_CONSECUTIVE_FETCH_FAILURES
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  def makeNewStageAttempt(
      ctx: ComponentContext,
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(ctx)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  def findMissingPartitions(): Seq[Int] = {
    val missing = _calcPartIds.indices.filter(id => partResults(id).isEmpty)
    assert(missing.size == _calcPartIds.length - numFinished,
      s"${missing.size} missing, expected ${_calcPartIds.length - numFinished}")
    missing
  }

  def findGlobalMissingPartitions(): Seq[Int] = Seq.empty
}

private[spark] object Stage {
  // The number of consecutive failures allowed before a stage is aborted
  val MAX_CONSECUTIVE_FETCH_FAILURES = 4

  def serializeWithDependencies(
    stage: Stage,
    jobId: Int,
    parts: Array[Int],
    properties: Properties,
    othBlockManagerIds: Array[BlockManagerId],
    currentFiles: mutable.Map[String, Long],
    currentJars: mutable.Map[String, Long],
    serializer: SerializerInstance)
  : ByteBuffer = {

    val out = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    dataOut.writeInt(jobId)

    dataOut.flush()
    val objOut = new ObjectOutputStream(out)

    objOut.writeInt(othBlockManagerIds.length)
    othBlockManagerIds.foreach(objOut.writeObject)

    objOut.writeObject(properties)

    objOut.writeInt(parts.length)
    parts.foreach(objOut.writeInt)

    objOut.flush()

    // Write the task itself and finish
    val stageBytes = serializer.serialize(stage)
    Utils.writeByteBuffer(stageBytes, out)
    out.close()
    out.toByteBuffer
  }

  def deserializeWithDependencies(serializedStage: ByteBuffer): (
    HashMap[String, Long],  // files
    HashMap[String, Long],  // jars
    Properties,             // properties
    Array[BlockManagerId],
    Array[Int],       // partitions
    Int,                    // jobId
    ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedStage)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val stageFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      stageFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val stageJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      stageJars(dataIn.readUTF()) = dataIn.readLong()
    }

    val jobId = dataIn.readInt()

    val objIn = new ObjectInputStream(in)

    val blockMIdLen = objIn.readInt()
    val blockMIds = Array.ofDim[BlockManagerId](blockMIdLen)
    (0 until blockMIdLen).foreach { idx =>
      blockMIds(idx) = objIn.readObject().asInstanceOf[BlockManagerId]
    }

    val stageProps = objIn.readObject().asInstanceOf[Properties]

    val aryLen = dataIn.readInt()
    val parts = (0 until aryLen).map(_ => objIn.readInt()).toArray

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedStage.slice()  // ByteBufferInputStream will have read just up to task
    (stageFiles, stageJars, stageProps, blockMIds, parts, jobId, subBuffer)
  }

  def serializeStageResult(
    stageId: Int,
    stageIdx: Int,
    results: Array[_]
  ): ByteBuffer = {
    val out = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    dataOut.writeInt(stageId)
    dataOut.writeInt(stageIdx)

    dataOut.writeInt(results.length)
    dataOut.flush()
    val objOut = new ObjectOutputStream(out)
    results.foreach(objOut.writeObject)

    objOut.flush()

    out.close()
    out.toByteBuffer
  }

  def deserializeStageResult(serializedResult: ByteBuffer): (Array[_], Int, Int) = {
    val in = new ByteBufferInputStream(serializedResult)
    val dataIn = new DataInputStream(in)

    val stageId = dataIn.readInt()
    val stageIdx = dataIn.readInt()

    val rstLen = dataIn.readInt()
    val results = Array.ofDim[Any](rstLen)
    val objIn = new ObjectInputStream(in)
    (0 until rstLen).foreach(idx => results(idx) = objIn.readObject())
    (results, stageIdx, stageId)
  }
}
