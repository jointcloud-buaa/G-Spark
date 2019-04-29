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

import scala.annotation.meta.{field, param}
import scala.collection.mutable.{Map => MMap}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    @(transient @param) parents: List[Stage],
    @(transient @param) firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],
    val fakeRDD: RDD[_])  // fakeShuffledRDD
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  @transient private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * List of [[MapStatus]] for each partition. The index of the array is the map partition id,
   * and each value in the array is the list of possible [[MapStatus]] for a partition
   * (a single task might run multiple times).
   */
  @transient private[this] var outputLocs: Array[List[MapStatus]] = _

  def initOutputLocs(subStageSize: Int): Unit = outputLocs = Array.fill(subStageSize)(Nil)

  override def toString: String = "ShuffleMapStage " + id

  // ==== 在GD中执行
  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   * This should be kept consistent as `outputLocs.filter(!_.isEmpty).size`.
   */
  def numAvailableOutputs: Int = numFinished

  override def findGlobalMissingPartitions(): Seq[Int] = {
    val missing = outputLocs.indices.filter(id => outputLocs(id).isEmpty)
    missing
  }

  def addOutputLoc(stageIdx: Int, status: MapStatus): Unit = {
    val prevList = outputLocs(stageIdx)
    outputLocs(stageIdx) = status :: prevList
    if (prevList == Nil) {
      numFinished += status.asInstanceOf[SimpleMapStatus].partitionsLen
    }
  }

  def removeOutputLoc(stageIdx: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(stageIdx)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(stageIdx) = newList
    if (prevList != Nil && newList == Nil) {
      numFinished -= prevList.head.asInstanceOf[SimpleMapStatus].partitionsLen
    }
  }

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   * This should be the same as `outputLocs.contains(Nil)`.
   */
  def isAvailable: Boolean = numFinished == numPartitions

  /**
   * Returns an array of [[MapStatus]] (index by partition id). For each partition, the returned
   * value contains only one (i.e. the first) [[MapStatus]]. If there is no entry for the partition,
   * that position is filled with null.
   */
  def outputLocInMapOutputTrackerFormat(): MMap[Int, MapStatus] = {
    MMap.empty ++ outputLocs.indices.zip(outputLocs.map(_.headOption.orNull))
  }

  // ==== 在SD中执行
  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (idx <- calcPartIds.indices) {
      val prevList = partResults(idx)
      val newList = prevList.filterNot(_.asInstanceOf[MapStatus].location.executorId == execId)
      partResults(idx) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numFinished -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numFinished, calcPartIds.length, isSiteAvailable))
    }
  }
}
