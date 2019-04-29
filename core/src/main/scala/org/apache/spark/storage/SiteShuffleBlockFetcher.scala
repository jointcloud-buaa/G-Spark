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

import java.io.{InputStream, IOException}

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.util.Utils

object SiteShuffleBlockFetcher extends Logging {

  private val conf: SparkConf = SparkEnv.get.conf
  private val blockManager: BlockManager = SparkEnv.get.blockManager
  private val serializerManager: SerializerManager = SparkEnv.get.serializerManager
  private val serializer: Serializer = SparkEnv.get.serializer
  private val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private val shuffleClient = blockManager.shuffleClient
  private val serializerInstance = serializer.newInstance()

  private val fileBufferSize = conf.getSizeAsKb(
    "spark.shuffle.file.buffer", "32k").toInt * 1024

  private val context = TaskContext.empty()

  // BlockId指RemoteShuffleBlockId, fetch操作本身就是异步的
  def fetchRemoteShuffleBlock(blockMId: BlockManagerId, blockIds: Array[BlockId]): Unit = {
    shuffleClient.fetchBlocks(
      blockMId.host, blockMId.port, blockMId.executorId, blockIds.map(_.toString),
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          val realBlockId = BlockId(blockId).asInstanceOf[RemoteShuffleBlockId]
          // 这里是SD的BlockManagerId
          val hostBlockId = HostAwareShuffleBlockId(
            blockMId.hostPort, realBlockId.shuffleId, realBlockId.reduceId)
          val rst = writeAsBlockFile(hostBlockId, buf)
          mapOutputTracker.registerRemoteShuffleFetchResult(
            realBlockId.shuffleId, realBlockId.reduceId, blockMId, rst)
        }

        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {

        }
      })
  }

  private def writeAsBlockFile(blockId: BlockId, buf: ManagedBuffer): (Boolean, Long) = {
    val newBlockId = blockId.asInstanceOf[HostAwareShuffleBlockId]
    var inputStream: InputStream = null
    var writeSuccess = false
    var dataLen: Long = 0
    try {
      inputStream = buf.createInputStream()
      val wrappedStreams = serializerManager.wrapStream(blockId, inputStream)
      val recordIter = serializerInstance.deserializeStream(wrappedStreams).asKeyValueIterator
      val dataFile = blockManager.diskBlockManager.getFile(newBlockId)
      val tmp = Utils.tempFileWith(dataFile)
      try {
        val writer = blockManager.getDiskWriter(newBlockId, tmp, serializerInstance,
          fileBufferSize, context.taskMetrics.shuffleWriteMetrics)
        while (recordIter.hasNext) {
          var cur = recordIter.next()
          writer.write(cur._1, cur._2)
        }
        val segment = writer.commitAndGet()
        writer.close()
        if (dataFile.exists()) dataFile.delete()
        if (tmp != null && tmp.exists() && !tmp.renameTo(dataFile)) {
          throw new IOException(s"fail to rename file $tmp to $dataFile")
        }
        writeSuccess = true
        dataLen = segment.length
      } finally {
        if (tmp.exists() && !tmp.delete()) {
          logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
        }
      }
    } finally {
      inputStream.close()
    }
    (writeSuccess, dataLen)
  }
}
