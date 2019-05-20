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

import scala.collection.mutable.{Map => MMap}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

class ApplicationStatisticsListener(statPath: String, appId: String)
  extends SparkListener with Logging {

  private var startTime: Long = _
  private var endTime: Long = _
  private var appName: String = _
  private var appJobStats: MMap[Int, JobStatData] = MMap.empty
  private var appStageStats: MMap[Int, StageStatData] = MMap.empty

  private val f = Paths.get(statPath, appId, "AppStats.csv").toFile
  if (!f.getParentFile.exists) {
    if (!f.getParentFile.mkdirs) {
      throw new SparkException(s"the dir $statPath does not exists and mkdir failed!")
    }
  }

  private val statFile = new BufferedWriter(new FileWriter(f))

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    startTime = applicationStart.time
    appName = applicationStart.appName
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    endTime = applicationEnd.time

    statFile.write(s"# App,$appName,$appId,${endTime-startTime}\n\n")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobStats = JobStatData(jobStart.jobId, jobStart.stageIds)
    jobStats.startTime = jobStart.time
    appJobStats(jobStart.jobId) = jobStats
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobStats = appJobStats(jobEnd.jobId)
    jobStats.endTime = jobEnd.time

    statFile.write(
      s"# Job,${jobStats.jobId},${jobStats.stages.length},${jobStats.spentTime}" + "\n\n")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val info = stageSubmitted.stageInfo
    val stageStats = StageStatData(
      info.name, info.stageId, stageSubmitted.subStageNum, info.stageType)
    appStageStats(info.stageId) = stageStats
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val stageStats = appStageStats(info.stageId)
    stageStats.startTime = info.submissionTime.get
    stageStats.endTime = info.completionTime.get

    statFile.write(s"# Stage,${stageStats.name},${stageStats.stageId},${stageStats.stageType}," +
      s"${stageStats.spentTime}")
  }

  def stop(): Unit = {
    statFile.close()
  }
}
