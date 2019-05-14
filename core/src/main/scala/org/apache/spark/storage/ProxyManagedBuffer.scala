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

import java.io.InputStream
import java.nio.ByteBuffer

import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.util.ThreadUtils

class ProxyManagedBuffer(
  blockId: RemoteShuffleBlockId,
  blockManagerId: BlockManagerId,
  shuffleClient: ShuffleClient
) extends ManagedBuffer {

  private val realBuf: Promise[ManagedBuffer] = Promise()

  private val shuffleBlockId = ShuffleBlockId(blockId.shuffleId, blockId.reduceId, blockId.reduceId)

  shuffleClient.fetchBlocks(
    blockManagerId.host, blockManagerId.port, blockManagerId.executorId,
    Array(shuffleBlockId.toString), new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
        data.retain()
        realBuf.success(data)
      }
      // TODO-lzp: 思考如何增加块获取失败后的重试，和异常处理
      override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
        realBuf.failure(exception)
      }
    }
  )

  override def size(): Long = {
    if (!realBuf.isCompleted) {
      ThreadUtils.awaitResult(realBuf.future, Duration.Inf)
    }
    realBuf.future.value.get.get.size()
  }

  override def nioByteBuffer(): ByteBuffer = {
    if (!realBuf.isCompleted) {
      ThreadUtils.awaitResult(realBuf.future, Duration.Inf)
    }
    realBuf.future.value.get.get.nioByteBuffer()
  }

  override def createInputStream(): InputStream = {
    if (!realBuf.isCompleted) {
      ThreadUtils.awaitResult(realBuf.future, Duration.Inf)
    }
    realBuf.future.value.get.get.createInputStream()
  }

  override def retain(): ManagedBuffer = {
    if (!realBuf.isCompleted) {
      ThreadUtils.awaitResult(realBuf.future, Duration.Inf)
    }
    realBuf.future.value.get.get.retain()
  }

  override def release(): ManagedBuffer = {
    if (!realBuf.isCompleted) {
      ThreadUtils.awaitResult(realBuf.future, Duration.Inf)
    }
    realBuf.future.value.get.get.release()
  }

  override def convertToNetty(): AnyRef = {
    if (!realBuf.isCompleted) {
      ThreadUtils.awaitResult(realBuf.future, Duration.Inf)
    }
    realBuf.future.value.get.get.convertToNetty()
  }

  override def toString: String = s"proxy managedBuffer for $blockId from $blockManagerId"
}
