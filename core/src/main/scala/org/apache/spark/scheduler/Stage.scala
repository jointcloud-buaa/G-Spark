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

import java.io.{DataInputStream, DataOutputStream}
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

  /** Set of jobs that this stage belongs to. */
  @transient val jobIds = new HashSet[Int]

  val pendingPartitions = new HashSet[Int]

  def init(parts: Array[Int]): Unit

  private var _calcPartitions: Array[Int] = _

  def calcPartitions_=(_partitions: Array[Int]): Unit = _calcPartitions = _partitions

  def calcPartitions: Array[Int] = _calcPartitions

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
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    // TODO-lzp: 这里会有问题的, 不能使用SparkContext
    metrics.register(rdd.sparkContext)
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
  def findMissingPartitions(): Seq[Int]

  def findGlobalMissingPartitions(): Seq[Int]

  def toDebugString(): String = {
    s"""the stage($id):
       |name: $name
       |numTasks: $numTasks
       |rdd: $rdd
       |numPartitions: $numPartitions
       |calcPartitions: $calcPartitions
       |pendingPartitions: $pendingPartitions
     """.stripMargin
  }
}

private[spark] object Stage {
  // The number of consecutive failures allowed before a stage is aborted
  val MAX_CONSECUTIVE_FETCH_FAILURES = 4

  def serializeWithDependencies(
    stage: Stage,
    jobId: Int,
    parts: Array[Int],
    properties: Properties,
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

    // Write the task properties separately so it is available before full task deserialization.
    val propBytes = Utils.serialize(properties)
    dataOut.writeInt(propBytes.length)
    dataOut.write(propBytes)

    dataOut.writeInt(parts.length)
    parts.foreach(i => dataOut.writeInt(i))

    dataOut.writeInt(jobId)

    // Write the task itself and finish
    dataOut.flush()
    val stageBytes = serializer.serialize(stage)
    Utils.writeByteBuffer(stageBytes, out)
    out.close()
    out.toByteBuffer
  }

  def deserializeWithDependencies(serializedStage: ByteBuffer)
  : (HashMap[String, Long], HashMap[String, Long], Properties, Array[Int], Int, ByteBuffer) = {

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

    val propLength = dataIn.readInt()
    val propBytes = new Array[Byte](propLength)
    dataIn.readFully(propBytes, 0, propLength)
    val stageProps = Utils.deserialize[Properties](propBytes)

    val aryLen = dataIn.readInt()
    val parts = Array.ofDim[Int](aryLen)
    (0 until aryLen).foreach(idx => parts(idx) = dataIn.readInt())

    val jobId = dataIn.readInt()

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedStage.slice()  // ByteBufferInputStream will have read just up to task
    (stageFiles, stageJars, stageProps, parts, jobId, subBuffer)
  }

  def serializeStageResult(
    stageId: Int,
    parts: Array[Int],
    results: Array[_],
    serializer: SerializerInstance
  ): ByteBuffer = {
    val out = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    dataOut.writeInt(stageId)

    dataOut.writeInt(parts.length)
    parts.foreach(dataOut.writeInt)

    dataOut.flush()
    val resultBytes = serializer.serialize(results)
    Utils.writeByteBuffer(resultBytes, out)

    out.close()
    out.toByteBuffer
  }

  def deserializeStageResult(
    serializedResult: ByteBuffer,
    serializer: SerializerInstance
  ): (Array[_], Array[Int], Int) = {
    val in = new ByteBufferInputStream(serializedResult)
    val dataIn = new DataInputStream(in)

    val stageId = dataIn.readInt()

    val partsLen = dataIn.readInt()
    val parts = Array.ofDim[Int](partsLen)
    (0 until partsLen).foreach(idx => parts(idx) = dataIn.readInt())

    val subBuffer = serializedResult.slice()
    val results = serializer.deserialize[Array[_]](subBuffer)
    (results, parts, stageId)
  }
}
