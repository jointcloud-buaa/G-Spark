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

import org.apache.spark.SparkEnv
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

object SiteShuffleBlockFetcherUtils extends Logging {
  def writeAsBlockFile(
    blockId: BlockId,
    buf: ManagedBuffer,
    fileBufferSize: Int,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    // 这个很关键，序列化和反序列化原则上尽量用同一个序列化器实例, 比如对于ShuffleDependency
    serializerInstance: SerializerInstance = SparkEnv.get.serializer.newInstance(),
    taskMetrics: TaskMetrics = TaskMetrics.empty  // TODO-lzp: 应该如何使用它
  ): (Boolean, Long) = {
    var inputStream: InputStream = null
    var writeSuccess = false
    var dataLen: Long = 0
    try {
      inputStream = buf.createInputStream()
      val wrappedStreams = serializerManager.wrapStream(blockId, inputStream)
      val recordIter = serializerInstance.deserializeStream(wrappedStreams).asKeyValueIterator
      val dataFile = blockManager.diskBlockManager.getFile(blockId)
      val tmp = Utils.tempFileWith(dataFile)
      try {
        val writer = blockManager.getDiskWriter(blockId, tmp, serializerInstance,
          fileBufferSize, taskMetrics.shuffleWriteMetrics)
        while (recordIter.hasNext) {
          val cur = recordIter.next()
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
