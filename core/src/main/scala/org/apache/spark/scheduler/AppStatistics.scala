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

import java.io.{BufferedWriter, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.{Map => MMap}

import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream}

sealed trait AppStatData {
  var startTime: Long = _
  var endTime: Long = _
  def spentTime: Long = endTime - startTime
}

case class JobStatData(jobId: Int, stages: Seq[Int]) extends AppStatData

case class StageStatData(stageId: Int, subStageNum: Int) extends AppStatData {
  private val subStages = Array.ofDim[SubStageReportData](subStageNum)
  private val fakeStages = Array.ofDim[SubStageReportData](subStageNum)

  def addSubStage(data: SubStageReportData): Unit = {
    if (data.isFake) {
      fakeStages(data.idx) = data
    } else {
      subStages(data.idx) = data
    }
  }
}

case class SubStageStatData(
  id: Int, idx: Int, attempt: Int, name: String, taskNum: Int) extends AppStatData {

  private val tasks = Array.fill[List[TaskStatData]](taskNum)(Nil)
  private val remoteShuffleMetrics =
    MMap.empty[String, (ShuffleReadMetrics, ShuffleWriteMetrics, Long)]
  private var numFinished: Int = _
  var isFake: Boolean = false

  var finished: Boolean = false

  def getAllTasks: Array[TaskStatData] = tasks.map(_.head)

  def addRemoteShuffleMetrics(
    hostPort: String,
    sread: ShuffleReadMetrics,
    swrite: ShuffleWriteMetrics,
    waitTime: Long): Unit = {
    remoteShuffleMetrics(hostPort) = (sread, swrite, waitTime)
  }

  def addTask(taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit = {
    val prevList = tasks(taskInfo.index)
    tasks(taskInfo.index) = TaskStatData(taskInfo, taskMetrics) :: prevList
    if (prevList == Nil && taskInfo.successful) numFinished += 1
  }

  def calcScheWaitTime(start: Long): Unit = {
    tasks.map(_.head).foreach(t => t.scheWaitTime = t.startTime - start)
  }

  // 获取集合的总和、均值和标准差。需要说明的是，对于特定测量项，不是所有值都有期待的意义。
  // 比如，对于fetchWaitTime，其均值表示每个任务在shuffle Read时平均等待块拉取的时间，其标准差反应这个时间
  // 在不同Task之间的波动，但其总和就没有意义，因为所有Task可能大部分是并行的，等待时间也是在并行等待。
  private def getStat(stat: Array[Long]): (Long, Double, Double) = {
    val sum = stat.sum
    val m = sum.toDouble / stat.length
    (sum, m, math.sqrt(stat.map(t => (t-m) * (t-m)).sum / stat.length))
  }

  def taskMetricDataStats: Array[(Long, Double, Double)] = allTaskMetricDatas.map(getStat)
  def allTaskMetricDatas: Array[Array[Long]] = {
    require(numFinished == tasks.length, "the stage's all task is not finished")
    val metrics = tasks.map(_.head.metrics.get)
    val sreads = metrics.map(_.shuffleReadMetrics)
    val swrites = metrics.map(_.shuffleWriteMetrics)
    val inputs = metrics.map(_.inputMetrics)
    val outputs = metrics.map(_.outputMetrics)
    Array(
      tasks.map(_.head.spentTime),  // Task执行花费的时间
      tasks.map(_.head.scheWaitTime),  // Task等待调度的时间
      sreads.map(_.fetchWaitTime),  // 获取块数据的等待时间，所有的块，包括集群内和集群外的远程块
      sreads.map(_.loopWaitBlockTime),  // 获取集群外Shuffle块的等待时间
      sreads.map(_.recordsRead),  // 读入的记录总数, 聚合前
      sreads.map(_.localBytesRead),  // 本地字节数
      sreads.map(_.localBlocksFetched),  // 本地块个数
      sreads.map(_.remoteBytesRead),  // 本集群,非本地字节数
      sreads.map(_.remoteBlocksFetched),  // 本集群,非本地块个数
      sreads.map(_.remoteClusterBytesFetched),  // 非本集群字节数
      sreads.map(_.remoteClusterBlocksFetched),  // 非本集群块个数
      swrites.map(_.bytesWritten),  // 写入的字节数
      swrites.map(_.recordsWritten),  // 写入的记录数
      inputs.map(_.bytesRead),  // 从外部数据源读取的字节数
      inputs.map(_.recordsRead),  // 从外部数据源读取的记录数
      outputs.map(_.bytesWritten),  // 向外部数据源写入的字节数
      outputs.map(_.recordsWritten),  // 向外部数据源写入的记录数
      metrics.map(_.memoryBytesSpilled),  // 从内存溢出到磁盘的字节数
      metrics.map(_.diskBytesSpilled), // 写入磁盘的总字节数
      metrics.map(_.peakExecutionMemory),  // 尖峰内存使用值
      metrics.map(_.resultSize)  // 任务的结果大小
    )
  }

  def remoteShuffleStats: Option[RemoteShuffleStat] = if (remoteShuffleMetrics.isEmpty) None
  else {
    val (sreads, swrites, waitTimes) = remoteShuffleMetrics.values.toArray.unzip3
    val stat = RemoteShuffleStat(
      sreads.map(_.remoteClusterBlocksFetched).sum,  // 拉取的集群外块的个数
      sreads.map(_.remoteClusterBytesFetched).sum,  // 拉取的集群外的字节数
      sreads.map(_.recordsRead).sum,  // 读入的集群外的记录总数
      swrites.map(_.bytesWritten).sum,  // 向SiteDriver写入的字节数
      swrites.map(_.recordsWritten).sum,  // 向SiteDriver写入的记录总数
      waitTimes.max  // 集群外拉取花费的最长时间
    )
    Some(stat)
  }

  def writeToLocalFile(out: BufferedWriter): Unit = {
    require(numFinished == tasks.length, "the stage's all task is not finished")
    val comment = s"# $name($id,$idx,$attempt): time($startTime,$endTime,$spentTime), " +
      s"$remoteShuffleStats"
    val colNames = tasks(0).head.statColNames
    out.write(colNames.mkString(",") + "\n")
    tasks.map(_.head).foreach { taskStat =>
        val colValues = taskStat.statColValues
        assert(colNames.length == colValues.length,
          "the colNames' length does not equals the values")
        out.write(colValues.mkString(",") + "\n")
    }
    out.write("\n")
    out.flush()
  }

  def shortReportData: SubStageReportData = SubStageReportData(
    id, idx, spentTime, isFake, tasks.length, taskMetricDataStats, remoteShuffleStats
  )
}

object SubStageStatData {
  def serializeToByteBuffer(stat: SubStageStatData): ByteBuffer = {
    val out = new ByteBufferOutputStream()
    val objOut = new ObjectOutputStream(out)
    objOut.writeObject(stat.shortReportData)
    objOut.flush()
    out.close()
    out.toByteBuffer
  }

  def deserializeToShortReportData(buf: ByteBuffer): SubStageReportData = {
    val in = new ByteBufferInputStream(buf)
    val objIn = new ObjectInputStream(in)
    val data = objIn.readObject().asInstanceOf[SubStageReportData]
    in.close()
    data
  }
}

case class SubStageReportData(
  id: Int,
  idx: Int,
  spentTime: Long,
  isFake: Boolean,
  taskNum: Int,
  taskMetrics: Array[(Long, Double, Double)],
  rmtShuffle: Option[RemoteShuffleStat]
)

case class RemoteShuffleStat(
  blocksFetched: Long,
  bytesFetched: Long,
  recordsRead: Long,
  bytesWritten: Long,
  recordsWritten: Long,
  waitTime: Long
)

case class TaskStatData(
  id: Long,
  idx: Int,
  attempt: Int,
  finished: Boolean,
  metrics: Option[TaskMetrics]
) extends AppStatData {
  var scheWaitTime: Long = _

  def statColNames: Seq[String] = Seq(
    "Id",
    "Index",
    "Attempt",
    "IsFinished",
    "SpentTime",
    "ScheWaitTime",
    "SRFetchWaitTime",
    "SRLoopWaitBlockTime",
    "SRRecordsRead",
    "SRLocalBytesRead",
    "SRLocalBlocksFetched",
    "SRRemoteBytesRead",
    "SRRemoteBlocksFetched",
    "SRRemoteClusterBytesFetched",
    "SRRemoteClusterBlocksFetched",
    "SWBytesWritten",
    "SWRecordsWritten",
    "InputBytesRead",
    "InputRecordsRead",
    "OutputBytesWritten",
    "OutputRecordsWritten",
    "MemoryBytesSpilled",
    "DiskBytesSpilled",
    "PeakExecutionMemory",
    "ResultSize"
  )

  def statColValues: Seq[AnyVal] = {
    val sread = metrics.get.shuffleReadMetrics
    val swrite = metrics.get.shuffleWriteMetrics
    val sinput = metrics.get.inputMetrics
    val soutput = metrics.get.outputMetrics
    Seq(
      id,
      idx,
      attempt,
      finished,
      spentTime,
      scheWaitTime,
      sread.fetchWaitTime,
      sread.loopWaitBlockTime,
      sread.recordsRead,
      sread.localBytesRead,
      sread.localBlocksFetched,
      sread.remoteBytesRead,
      sread.remoteBlocksFetched,
      sread.remoteClusterBytesFetched,
      sread.remoteClusterBlocksFetched,
      swrite.bytesWritten,
      swrite.recordsWritten,
      sinput.bytesRead,
      sinput.recordsRead,
      soutput.bytesWritten,
      soutput.recordsWritten,
      metrics.get.memoryBytesSpilled,
      metrics.get.diskBytesSpilled,
      metrics.get.peakExecutionMemory,
      metrics.get.resultSize
    )
  }

}

object TaskStatData {
  def apply(taskInfo: TaskInfo, taskMetrics: TaskMetrics): TaskStatData = {
    val stat = new TaskStatData(
      taskInfo.taskId, taskInfo.index, taskInfo.attemptNumber, taskInfo.successful,
      Option(taskMetrics)
    )
    stat.startTime = taskInfo.launchTime
    stat.endTime = taskInfo.finishTime
    stat
  }
}
