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

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    execId: String,
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: Option[LiveListenerBus])
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  // 这里考虑到, GlobalDriver上应该是没有拓扑信息的
  private val topologyMapper: Option[TopologyMapper] = if (Utils.isSDriver(execId)) {
    val topologyMapperClassName = conf.get(
      "spark.storage.replication.topologyMapper", classOf[DefaultTopologyMapper].getName)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    Some(mapper)
  } else None

  logInfo("BlockManagerMasterEndpoint up")

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, maxMemSize, masterEndpoint, slaveEndpoint) =>
      context.reply(register(blockManagerId, maxMemSize, masterEndpoint, slaveEndpoint))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      listenerBus.foreach(_.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo))))

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    case RemoveRdd(rddId, level) =>
        context.reply(removeRdd(rddId, level))

    case RemoveShuffle(shuffleId, level) =>
        context.reply(removeShuffle(shuffleId, level))

    case RemoveBroadcast(broadcastId, initLevel, downLevel, removeFromDriver, level) =>
        context.reply(removeBroadcast(broadcastId, initLevel, downLevel, removeFromDriver, level))

    case RemoveBlock(blockId, level) =>
      removeBlockFromWorkers(blockId, level)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case RemoveSiteDriver(sdriverId) =>
      removeSiteDriver(sdriverId)
      context.reply(true)

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case BlockManagerSiteHeartbeat(blockManagerId) =>
      context.reply(siteHeartbeatReceived(blockManagerId))

    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
  }

  private def removeRdd(rddId: Int, level: Int): Future[Seq[(Option[Int], Int)]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    // 移除本地保存的一些块的追踪信息, 不是真正的块数据
    //                                              asRDDId为Option[RDDBlockId], 所以需要flatMap
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    val removeMsg = RemoveRdd(rddId, level + 1)
    // TODO-lzp: 可能会出错的地方在于，多级信息获取时的超时，特别是GD/SD的超时要设置的较长
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        // masterEndpoint返回的类型是Seq[(Option[Int], Int)]，此处将其相加
        val m = bm.masterEndpoint.map(
          _.askWithRetry[Future[Seq[(Option[Int], Int)]]](removeMsg).map(
            _.map{case (v1, v2) => v1.getOrElse(0) + v2}.sum
          )
        )
        val s = bm.slaveEndpoint.ask[Int](removeMsg)
        m match {
          case Some(mv) =>
            mv.flatMap(mm => s.map(ss => (Some(mm), ss)))
          case None =>
            s.map(ss => (None, ss))
        }
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int, level: Int): Future[Seq[(Option[Boolean], Boolean)]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId, level + 1)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        val m = bm.masterEndpoint.map(
          _.askWithRetry[Future[Seq[(Option[Boolean], Boolean)]]](removeMsg).map(
            // v1为从每个节点的masterEndpoint获取的值，其为None表示没有master，则默认为true
            _.map{case (v1, v2) => v1.getOrElse(true) && v2}.forall(b => b)
          )
        )
        val s = bm.slaveEndpoint.ask[Boolean](removeMsg)
        m match {
          case Some(mv) =>
            mv.flatMap(mm => s.map(ss => (Some(mm), ss)))
          case None =>
            s.map(ss => (None, ss))
        }
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  // 这里的removeFromDriver应该理解为是否移除level为0时的当前节点的广播变量, 除此外，当level>0时，应该移除
  // 所有广播变量. 则不从driver移除的条件是：不允许移除 且 level为0 且 不为当前节点
  private def removeBroadcast(
    broadcastId: Long, initLevel: Int, downLevel: Int,
    removeFromDriver: Boolean, level: Int): Future[Seq[(Option[Int], Int)]] = {
    val removeMsg = RemoveBroadcast(broadcastId, initLevel, downLevel, removeFromDriver, level + 1)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      // 要么允许从driver中移除，要么level不为0，要么不为当前节点
      removeFromDriver || level != 0 || info.blockManagerId.executorId != execId
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        val s = bm.slaveEndpoint.ask[Int](removeMsg)
        // TODO-lzp: 此处指定传播指定层级
        if (level == 0) {
          val m = bm.masterEndpoint.map(
            _.askWithRetry[Future[Seq[(Option[Int], Int)]]](removeMsg).map(
              _.map{case (v1, v2) => v1.getOrElse(0) + v2}.sum
            )
          )
          m match {
            case Some(mv) =>
              mv.flatMap(mm => s.map(ss => (Some(mm), ss)))
            case None =>
              s.map(ss => (None, ss))
          }
        } else {
          s.map(ss => (None, ss))
        }
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.size == 0) {
        blockLocations.remove(blockId)
      }
    }
    listenerBus.foreach(_.post(
      SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId)))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  private def removeSiteDriver(execId: String): Unit = {
    logInfo("Trying to remove siteDriver " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  private def siteHeartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isSiteDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  // 这里很有意思，如果是GD，则此处的slaveEndpoint是SD的driverEndpoint，然后又会回到这里，一级级传递下去
  private def removeBlockFromWorkers(blockId: BlockId, level: Int) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          // 这里也有意思，GD的slaveEndpoint是SlaveEndpoint，而SD的slaveEndpoint是MasterEndpoint
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId, level + 1))
          blockManager.get.masterEndpoint.foreach(_.ask[Boolean](RemoveBlock(blockId, level + 1)))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          // 此处特意不向下一级的masterEndpoint询问,因为假设了块的状态只会存在于连续的两个层级间
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  /**
   * Returns the BlockManagerId with topology information populated, if available.
   */
  private def register(
      idWithoutTopologyInfo: BlockManagerId,
      maxMemSize: Long,
      masterEndpoint: Option[RpcEndpointRef],
      slaveEndpoint: RpcEndpointRef): BlockManagerId = {

    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.flatMap(_.getTopologyForHost(idWithoutTopologyInfo.host)))

    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, masterEndpoint, slaveEndpoint)
    }
    listenerBus.foreach(_.post(SparkListenerBlockManagerAdded(time, id, maxMemSize)))
    id
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.executorId == execId && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    logInfo(s"##lizp##: in $execId in getLocations: get $blockId")
    logInfo(s"##lizp##: all blocks is " + blockLocations.keySet())
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { bmId =>
        // 当前节点是GD，则过滤GD, 当前节点是SD，则过滤SD
        (Utils.isGDriver(execId) && bmId.isDriver) ||
          (Utils.isSDriver(execId) && bmId.isSiteDriver)
      }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String)
  : Option[(Option[RpcEndpointRef], RpcEndpointRef)] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      (info.masterEndpoint, info.slaveEndpoint)
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxMem: Long,
  val masterEndpoint: Option[RpcEndpointRef],
    val slaveEndpoint: RpcEndpointRef)
  extends Logging {

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    updateLastSeenMs()

    if (_blocks.containsKey(blockId)) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
    } else if (_blocks.containsKey(blockId)) {
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
