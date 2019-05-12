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

import scala.collection.Iterable
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private[spark] trait BlockManagerMaster extends Logging {
  val execId: String

  def stop(): Unit = {}
}

private[spark] trait BMMMasterRole extends BlockManagerMaster {
  val conf: SparkConf

  var localRef: RpcEndpointRef

  val timeout = RpcUtils.askRpcTimeout(conf)

  // 移除下一级组件，对GD来说是SD，对SD来说是E，限制传播层级为1
  def removeExecutor(executorId: String): Unit = {
    tellSelf(RemoveExecutor(executorId))
    logInfo(s"Removed $executorId successfully in removeExecutor")
  }

  def removeExecutorAsync(executorId: String): Unit = {
    localRef.ask[Boolean](RemoveExecutor(executorId))
    logInfo(s"Removal of executor $executorId requested")
  }

  private def tellSelf(msg: Any): Unit = {
    if (!localRef.askWithRetry[Boolean](msg)) {
      throw new SparkException("BlockManagerMasterEndpoint returned false, expected true.")
    }
  }

  def registerLocalBlockManager(
    bmId: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    logInfo(s"Registering BlockManager $bmId")
    val updateId = localRef.askWithRetry[BlockManagerId](
      //                                   为避免循环引用，此处为None
      RegisterBlockManager(bmId, maxMemSize, None, slaveEndpoint))
    logInfo(s"Registered BlockManager $updateId")
    updateId
  }

  def updateLocalBlockInfo(
    blockManagerId: BlockManagerId,
    blockId: BlockId,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long): Boolean = {
    val res = localRef.askWithRetry[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logDebug(s"Updated info of block $blockId")
    res
  }

  def getLocalLocations(blockId: BlockId): Seq[BlockManagerId] = {
    localRef.askWithRetry[Seq[BlockManagerId]](GetLocations(blockId))
  }

  def getLocalLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    localRef.askWithRetry[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    localRef.askWithRetry[Seq[BlockManagerId]](GetPeers(blockManagerId))
  }

  // 强调，获取下一级的Ref, 下一级是相对的
  def getExecutorEndpointRef(executorId: String)
  : Option[(Option[RpcEndpointRef], RpcEndpointRef)] = {
    localRef.askWithRetry[Option[(Option[RpcEndpointRef], RpcEndpointRef)]](
      GetExecutorEndpointRef(executorId)
    )
  }

  def removeBlock(blockId: BlockId): Unit = {
    localRef.askWithRetry[Boolean](RemoveBlock(blockId, 0))
  }

  def removeRdd(rddId: Int, blocking: Boolean): Unit = {
    val future = localRef.askWithRetry[Future[Seq[(Option[Int], Int)]]](RemoveRdd(rddId, 0))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove RDD $rddId - ${e.getMessage}", e)
    }(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  def removeShuffle(shuffleId: Int, blocking: Boolean) {
    // 虽然要返回值，但是其实并没有使用
    val future = localRef.askWithRetry[Future[Seq[(Option[Boolean], Boolean)]]](
      RemoveShuffle(shuffleId, 0))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove shuffle $shuffleId - ${e.getMessage}", e)
    }(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  def removeBroadcast(broadcastId: Long, initLevel: Int, downLevel: Int,
    removeFromMaster: Boolean, blocking: Boolean) {
    val future = localRef.askWithRetry[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, initLevel, downLevel, removeFromMaster, 0))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove broadcast $broadcastId" +
          s" with removeFromMaster = $removeFromMaster - ${e.getMessage}", e)
    }(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    localRef.askWithRetry[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  def getStorageStatus: Array[StorageStatus] = {
    localRef.askWithRetry[Array[StorageStatus]](GetStorageStatus)
  }

  // 用于测试
  def getBlockStatus(
    blockId: BlockId,
    askSlaves: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askSlaves)
    /*
     * To avoid potential deadlocks, the use of Futures is necessary, because the master endpoint
     * should not block on waiting for a block manager, which can in turn be waiting for the
     * master endpoint for a response to a prior message.
     */
    val response = localRef.askWithRetry[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    implicit val sameThread = ThreadUtils.sameThread
    val cbf =
      implicitly[
        CanBuildFrom[Iterable[Future[Option[BlockStatus]]],
          Option[BlockStatus],
          Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence[Option[BlockStatus], Iterable](futures)(cbf, ThreadUtils.sameThread))
    if (blockStatus == null) {
      throw new SparkException("BlockManager returned null for BlockStatus query: " + blockId)
    }
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  def getMatchingBlockIds(
    filter: BlockId => Boolean,
    askSlaves: Boolean): Seq[BlockId] = {
    val msg = GetMatchingBlockIds(filter, askSlaves)
    val future = localRef.askWithRetry[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
  }

  def hasCachedBlocks(executorId: String): Boolean = {
    localRef.askWithRetry[Boolean](HasCachedBlocks(executorId))
  }

  def contains(blockId: BlockId): Boolean = getLocalLocations(blockId).nonEmpty

  override def stop(): Unit = {
    if (localRef != null) {
      tellSelf(StopBlockManagerMaster)
      localRef = null
      logInfo("BlockManagerMaster stopped")
    }
  }
}

private[spark] trait BMMWorkerRole extends BlockManagerMaster {
  var driverEndpoint: RpcEndpointRef

  def registerBlockManager(
    bmId: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    logInfo(s"Registering BlockManager $bmId")
    val updateId = driverEndpoint.askWithRetry[BlockManagerId](
      //                                  Worker角色本身不启动MasterEndpoint，所以为None
      RegisterBlockManager(bmId, maxMemSize, None, slaveEndpoint))
    logInfo(s"Registered BlockManager $updateId")
    updateId
  }

  // 有必要声明下，所有块的状态更新，都只会向上更新一级，不会无限地往上更新
  // TODO-lzp: 会不会有全局的块更新需求
  def updateBlockInfo(
    blockManagerId: BlockManagerId,
    blockId: BlockId,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long): Boolean = {
    val res = driverEndpoint.askWithRetry[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logDebug(s"Updated info of block $blockId")
    res
  }

  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    driverEndpoint.askWithRetry[Seq[BlockManagerId]](GetLocations(blockId))
  }

  def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    driverEndpoint.askWithRetry[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

}

private[spark] trait BMMMiddleRole extends BMMMasterRole with BMMWorkerRole {

  override def registerBlockManager(
    bmId: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    logInfo(s"Registering BlockManager $bmId")
    val updateId = driverEndpoint.askWithRetry[BlockManagerId](
      RegisterBlockManager(bmId, maxMemSize, Some(localRef), slaveEndpoint))
    logInfo(s"Registered BlockManager $updateId")
    updateId
  }
}

private[spark] class BMMGlobalMaster(
  val execId: String,
  var localRef: RpcEndpointRef,
  val conf: SparkConf
) extends BMMMasterRole

private[spark] class BMMMaster(
  val execId: String,
  var localRef: RpcEndpointRef,
  var driverEndpoint: RpcEndpointRef,
  val conf: SparkConf
) extends BMMMiddleRole

private[spark] class BMMWorker(
  val execId: String,
  var driverEndpoint: RpcEndpointRef
) extends BMMWorkerRole

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "BlockManagerMaster"
}
