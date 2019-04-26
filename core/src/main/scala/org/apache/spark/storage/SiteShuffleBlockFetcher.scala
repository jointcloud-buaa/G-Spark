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

import scala.collection.mutable.{Map => MMap}

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.BlockFetchingListener

class SiteShuffleBlockFetcher(
  blockManager: BlockManager,
  blockManagerIds: BlockManagerId,
  blockIds: Array[BlockId]
) {

  private val shuffleClient = blockManager.shuffleClient

  private val fetchStatus: MMap[BlockId, Boolean] = MMap.empty

  // TODO-lzp: 增加重试
  def fetchRemoteShuffleBlock(blockMId: BlockManagerId, blockIds: Array[BlockId]): Unit = {
    shuffleClient.fetchBlocks(
      blockMId.host, blockMId.port, blockMId.executorId, blockIds.map(_.toString),
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          SiteShuffleBlockFetcher.this.synchronized {
            fetchStatus(BlockId(blockId)) = true
            // TODO-lzp: 写入文件, 并注册到MapOutputTracker
          }
        }

        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {

        }
      })
  }

}
