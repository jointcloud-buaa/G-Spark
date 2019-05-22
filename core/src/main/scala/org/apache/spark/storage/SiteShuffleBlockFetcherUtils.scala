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
package org.apache.spark.storage

import java.io.IOException

import scala.collection.mutable.{Map => MMap}

import org.apache.spark.{InterruptibleIterator, MapOutputTracker, MapOutputTrackerMasterRole, SparkConf, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{Serializer, SerializerInstance, SerializerManager}
import org.apache.spark.util.{CompletionIterator, Utils}

object SiteShuffleBlockFetcherUtils extends Logging {

  def fetchRemoteShuffleBlocks(
    conf: SparkConf,
    context: TaskContext,
    blocksByAddress: (BlockManagerId, Seq[(RemoteShuffleBlockId, Long)]),
    shuffleIdToSerializer: Map[Int, Serializer],
    blockManager: BlockManager = SparkEnv.get.blockManager,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker
  ): (ShuffleReadMetrics, ShuffleWriteMetrics) = {
    val maxBytesInFlight = conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024
    val maxReqsInFlight = conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue)
    val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

    val shuffleIdToInstance: MMap[Int, SerializerInstance] = MMap.empty

    try {
      // 拉取数据
      val blockFetcherItr = new ShuffleBlockFetcherIterator(
        context,
        blockManager.shuffleClient,
        blockManager,
        Seq(blocksByAddress),
        None,
        maxBytesInFlight,
        maxReqsInFlight
      )
      val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
      blockFetcherItr.foreach { case (blockId, inputStream) =>
        val newBlockId = blockId.asInstanceOf[RemoteShuffleBlockId]
        val wrappedStreams = serializerManager.wrapStream(blockId, inputStream)
        if (!shuffleIdToInstance.contains(newBlockId.shuffleId)) {
          shuffleIdToInstance(newBlockId.shuffleId) =
            shuffleIdToSerializer(newBlockId.shuffleId).newInstance()
        }
        val recordIter = shuffleIdToInstance(newBlockId.shuffleId)
          .deserializeStream(wrappedStreams).asKeyValueIterator
        val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
          recordIter.map { record =>
            readMetrics.incRecordsRead(1)
            record
          }, ()
        )
        val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
        // 写数据
        val hostBlockId = HostAwareShuffleBlockId(
          blocksByAddress._1.hostPort, newBlockId.shuffleId, newBlockId.reduceId
        )
        val rst = writeAsBlockFile(
          hostBlockId, interruptibleIter, fileBufferSize, blockManager,
          shuffleIdToInstance(newBlockId.shuffleId), context.taskMetrics()
        )
        // 更新结果
        mapOutputTracker.asInstanceOf[MapOutputTrackerMasterRole]
          .registerRemoteShuffleFetchResult(
            newBlockId.shuffleId, newBlockId.reduceId, blocksByAddress._1, rst
          )
      }
      context.taskMetrics().mergeShuffleReadMetrics()
    } finally {
      // 结束task，清理fetcher中的一些结果
      context.asInstanceOf[TaskContextImpl].markTaskCompleted()
    }
    (context.taskMetrics().shuffleReadMetrics, context.taskMetrics().shuffleWriteMetrics)
  }

  def writeAsBlockFile(
    blockId: BlockId,
    recordIter: Iterator[(Any, Any)],
    fileBufferSize: Int,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    // 这个很关键，序列化和反序列化原则上尽量用同一个序列化器实例, 比如对于ShuffleDependency
    serializerInstance: SerializerInstance = SparkEnv.get.serializer.newInstance(),
    taskMetrics: TaskMetrics = TaskMetrics.empty  // TODO-lzp: 应该如何使用它
  ): (Boolean, Long) = {
    var writeSuccess = false
    var dataLen: Long = 0
    val dataFile = blockManager.diskBlockManager.getFile(blockId)
    val tmp = Utils.tempFileWith(dataFile)
    var writer: DiskBlockObjectWriter = null
    try {
      writer = blockManager.getDiskWriter(blockId, tmp, serializerInstance,
        fileBufferSize, taskMetrics.shuffleWriteMetrics)
      while (recordIter.hasNext) {
        val cur = recordIter.next()
        writer.write(cur._1, cur._2)
      }
      val segment = writer.commitAndGet()
      if (dataFile.exists()) dataFile.delete()
      if (tmp != null && tmp.exists() && !tmp.renameTo(dataFile)) {
        throw new IOException(s"fail to rename file $tmp to $dataFile")
      }
      writeSuccess = true
      dataLen = segment.length
    } finally {
      writer.close()
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
    (writeSuccess, dataLen)
  }
}
