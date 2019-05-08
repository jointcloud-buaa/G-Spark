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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils

class FakeTask(
  stageId: Int,
  stageAttemptId: Int,
  taskBinary: Broadcast[Array[Byte]],
  partition: Partition,
  @transient private var locs: Seq[TaskLocation],
  val outputId: Int, // 用来索引Stage中的calcPartIds和partResults
  metrics: TaskMetrics,
  localProperties: Properties,
  jobId: Option[Int] = None,
  appId: Option[String] = None,
  appAttemptId: Option[String] = None
) extends Task[MapStatus](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
  appId, appAttemptId) with Logging {

  override def isFakeTask: Boolean = true

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq // 去重
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    // 反序列化广播变量taskBinary
    // 此处调用了taskBinary的value, 继而开始获取广播变量
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val fileBufferSize = SparkEnv.get.conf.getSizeAsKb(
      "spark.shuffle.file.buffer", "32k").toInt * 1024
    val shuffleManager = SparkEnv.get.shuffleManager
    val blockManager = SparkEnv.get.blockManager
    val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      .asInstanceOf[IndexShuffleBlockResolver]

    val dataFile = shuffleBlockResolver.getDataFile(dep.shuffleId, partitionId)
    val tmp = Utils.tempFileWith(dataFile)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, partitionId,
        IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val writer = blockManager.getDiskWriter(
        blockId, tmp, dep.serializer.newInstance(), fileBufferSize,
        context.taskMetrics().shuffleWriteMetrics
      )
      val iter = rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
      while (iter.hasNext) {
        var cur = iter.next()
        writer.write(cur._1, cur._2)
      }
      val segment = writer.commitAndGet()
      writer.close()

      val partitionLengths = Array.tabulate(dep.partitioner.numPartitions) { idx =>
        if (idx == partitionId) segment.length else 0
      }
      // 此处会写入IndexFile, 以及重命名dataFile
      shuffleBlockResolver.writeIndexFileAndCommit(
        dep.shuffleId, partitionId, partitionLengths, tmp
      )
      new SingleMapStatus(blockManager.shuffleServerId, partitionId, segment.length)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  // stageId, partitionId才是标识Task的属性
  override def toString: String = "FakeMapTask(%d, %d)".format(stageId, partitionId)
}
