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

import scala.collection.mutable.{Map => MMap}

class ApplicationStatisticsListener extends SparkListener {
  private var startTime: Long = _
  private var endTime: Long = _
  private var appName: String = _
  private var appJobStats: MMap[Int, JobStatData] = MMap.empty
  private var appStageStats: MMap[Int, StageStatData] = MMap.empty

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    startTime = applicationStart.time
    appName = applicationStart.appName
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    endTime = applicationEnd.time
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobStats = JobStatData(jobStart.jobId, jobStart.stageIds)
    jobStats.startTime = jobStart.time
    appJobStats(jobStart.jobId) = jobStats
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobStats = appJobStats(jobEnd.jobId)
    jobStats.endTime = jobEnd.time
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val info = stageSubmitted.stageInfo
    val stageStats = StageStatData(info.stageId, stageSubmitted.subStageNum)
    stageStats.startTime = info.submissionTime.get
    appStageStats(info.stageId) = stageStats
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val stageStats = appStageStats(info.stageId)
    stageStats.endTime = info.completionTime.get
  }

  override def onSubStageDataReport(report: SparkListenerSubStageDataReport): Unit = {
    val data = report.data

  }
}
