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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.siteDriver.SiteSchedulerBackend

class SiteAppStatisticsListener(
  statPath: String, siteAppId: String, backend: SiteSchedulerBackend
) extends SparkListener with Logging {

  var startTime: Long = _
  var endTime: Long = _

  def spentTime: Long = endTime - startTime

  private val sdriverSubStageStats: MMap[Int, ArrayBuffer[SubStageStatData]] = MMap.empty
  private val sdriverFakeStageStats: MMap[Int, ArrayBuffer[SubStageStatData]] = MMap.empty

  private val f = Paths.get(statPath, siteAppId, "subStageStats.data").toFile
  if (!f.getParentFile.exists) {
    if (!f.getParentFile.mkdirs) {
      throw new SparkException(s"the dir $statPath does not exists and mkdir failed!")
    }
  }

  private val statFile = new BufferedWriter(new FileWriter(f))

  override def onSiteAppStart(siteAppStart: SparkListenerSiteAppStart): Unit = {
    startTime = siteAppStart.startTime
  }

  override def onSiteAppEnd(siteAppEnd: SparkListenerSiteAppEnd): Unit = {
    endTime = siteAppEnd.time
  }

  override def onSubStageSubmitted(subStageSubmitted: SparkListenerSubStageSubmitted): Unit = {
    val info = subStageSubmitted.stageInfo
    val subStageStats = SubStageStatData(
      info.stageId, subStageSubmitted.stageIdx, info.attemptId,
      info.name, subStageSubmitted.taskNum
    )
    subStageStats.isFake = info.isFake
    if (info.isFake) {
      sdriverFakeStageStats(info.stageId) = sdriverFakeStageStats.getOrElse(
        info.stageId, ArrayBuffer.empty) += subStageStats
    } else {
      sdriverSubStageStats(info.stageId) = sdriverSubStageStats.getOrElse(
        info.stageId, ArrayBuffer.empty) += subStageStats
    }
  }

  override def onSubStageCompleted(subStageCompleted: SparkListenerSubStageCompleted): Unit = {
    val info = subStageCompleted.stageInfo
    val stat = if (info.isFake) {
      sdriverFakeStageStats(info.stageId)(info.attemptId)
    } else {
      sdriverSubStageStats(info.stageId)(info.attemptId)
    }
    // subStage的提交时间是在Event之后赋值的
    stat.startTime = info.submissionTime.get
    stat.endTime = info.completionTime.get
    stat.finished = true
    stat.calcScheWaitTime(stat.startTime)

    stat.writeToLocalFile(statFile)
    backend.reportSubStageStats(SubStageStatData.serializeToByteBuffer(stat))
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    val subStageStats = if (info.isFake) {
      sdriverFakeStageStats(taskEnd.stageId)(taskEnd.stageAttemptId)
    } else {
      sdriverSubStageStats(taskEnd.stageId)(taskEnd.stageAttemptId)
    }
    subStageStats.addTask(taskEnd.taskInfo, taskEnd.taskMetrics)
  }

  override def onRemoteShuffleFetchCompleted(
    rmtShuffleCompleted: SparkListenerRemoteShuffleFetchCompleted): Unit = {
    val subStageStats = sdriverSubStageStats(
      rmtShuffleCompleted.stageId)(rmtShuffleCompleted.stageAttemptId)
    subStageStats.addRemoteShuffleMetrics(
      rmtShuffleCompleted.hostPort,
      rmtShuffleCompleted.rmtReadMetrics,
      rmtShuffleCompleted.rmtWriteMetrics,
      rmtShuffleCompleted.waitTime
    )
  }

  def stop(): Unit = {
    statFile.close()
  }
}
