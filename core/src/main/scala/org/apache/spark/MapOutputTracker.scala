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

package org.apache.spark

import java.io._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap => SHashMap, HashSet, Map => MMap}
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, HostAwareShuffleBlockId, ShuffleBlockId}
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case class GetRemoteShuffleStatuses(requestId: Long, shuffleId: Int, reduceId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] trait GetStatusMessage {
  val shuffleId: Int
  val context: RpcCallContext
}
private[spark] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)
  extends GetStatusMessage
private[spark] case class GetRemoteShuffleMessage(requestId: Long, shuffleId: Int, reduceId: Int,
  context: RpcCallContext) extends GetStatusMessage

/** RpcEndpoint class for MapOutputTrackerMaster */
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMasterRole, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      tracker.post(GetMapOutputMessage(shuffleId, context))

    case GetRemoteShuffleStatuses(requestId: Long, shuffleId: Int, reduceId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo(s"Asked to send remote shuffle statuses for shuffle $shuffleId and " +
        s"reduce $reduceId with request $requestId to $hostPort")
      tracker.post(GetRemoteShuffleMessage(requestId, shuffleId, reduceId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

private[spark] trait MapOutputTracker extends Logging {

  def unregisterShuffle(shuffleId: Int): Unit

  /** Stop the tracker. */
  def stop(): Unit
}

private[spark] trait MapOutputTrackerMasterRole extends MapOutputTracker {
  protected val conf: SparkConf
  // 用于从序列化后的mapStatuses创建广播变量
  protected val broadcastManager: BroadcastManager
  protected val isLocal: Boolean

  // 指向自己的Endpoint
  var trackerEndpoint: RpcEndpointRef = _

  protected var epoch: Long = 0

  protected val epochLock = new AnyRef

  /** Called to get current epoch number. */
  def getEpoch: Long = epochLock.synchronized { epoch }

  def incrementEpoch(): Unit = epochLock.synchronized {
    epoch += 1
    logDebug("Increasing epoch to " + epoch)
  }

  // TODO-lzp: 丑陋。保存当前集群的SiteDriver的BlockManagerId, 在MOTWorker获取远程Shuffle块状态时，将
  //           远程块替换为本集群的BlockManagerId
  private var sdriverBlockManagerId: BlockManagerId = _
  def setBlockManagerId(bmId: BlockManagerId): Unit = sdriverBlockManagerId = bmId

  private var cacheEpoch = epoch

  /** Whether to compute locality preferences for reduce tasks */
  protected val shuffleLocalityEnabled =
    conf.getBoolean("spark.shuffle.reduceLocality.enabled", true)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  protected val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  protected val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task. Making this larger will focus on fewer locations where most data
  // can be read locally, but may lead to more delay in scheduling if those locations are busy.
  protected val REDUCER_PREF_LOCS_FRACTION = 0.2

  private val minSizeForBroadcast =
    conf.getSizeAsBytes("spark.shuffle.mapOutput.minSizeForBroadcast", "512k").toInt

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // 存储自己的主动存入的状态, 注意：外层的Map是并发安全的，内层的不是
  protected val mapStatuses = new ConcurrentHashMap[Int, MMap[Int, MapStatus]]().asScala
  protected val cachedSerializedStatuses = new ConcurrentHashMap[Int, Array[Byte]]().asScala

  protected val cachedSerializedBroadcast = new SHashMap[Int, Broadcast[Array[Byte]]]()

  protected val shuffleIdLocks = new ConcurrentHashMap[Int, AnyRef]()

  private val mapOutputRequests = new LinkedBlockingQueue[GetStatusMessage]

  // 在执行Task时，为增量地在MOTMaster与MOTWorker间传输远程Shuffle块状态设置的请求ID, 每个Task
  // 第一次请求时初始化，Task与Task间不同，ID唯一。每个Task只处理一个分区，而这一分区会对应不同
  // BlockManagerId上的数据块
  private val requestIdToBmIds =
    new ConcurrentHashMap[Long, mutable.HashSet[BlockManagerId]]().asScala

  private var nextRequestId = new AtomicLong(0)

  // 保存远程Shuffle的块状态。当SiteDriver从另一个SiteDriver拉取到数据块后，会存储到本地磁盘，并更新此处
  // shuffleId -> blockManagerId -> 以reduce分区为索引的数组, 元素值分别表示是否拉取成功，块大小
  protected val remoteShuffleFetchStatus =
    new ConcurrentHashMap[Int, MMap[BlockManagerId, Array[(Boolean, Long)]]].asScala

  protected def askTracker[T: ClassTag](message: Any): T = try {
      trackerEndpoint.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }

  protected def sendTracker(message: Any): Unit = {
    val response = askTracker[Boolean](message)
    if (!response) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  // 统计每个reduce分区的大小
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    val statuses = getStatuses(dep.shuffleId)
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized {
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      for ((_, s) <- statuses) {
        for (i <- totalSizes.indices) {
          totalSizes(i) += s.getSizeForBlock(i)
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
  }

  // 获取指定shuffleId的本地的mapStatus
  def getStatuses(shuffleId: Int): Map[Int, MapStatus] = mapStatuses(shuffleId).toMap

  def registerShuffle(shuffleId: Int): Unit = {
    if (mapStatuses.put(shuffleId, MMap.empty[Int, MapStatus]).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    shuffleIdLocks.putIfAbsent(shuffleId, new Object)
  }

  override def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
    cachedSerializedStatuses.remove(shuffleId)
    cachedSerializedBroadcast.remove(shuffleId).foreach(v => removeBroadcast(v))
    shuffleIdLocks.remove(shuffleId)
  }

  protected def removeBroadcast(bcast: Broadcast[_]): Unit = {
    if (null != bcast) {
      broadcastManager.unbroadcast(bcast.id,
        removeFromDriver = true, blocking = false)
    }
  }

  private def clearCachedBroadcast(): Unit = {
    for (cached <- cachedSerializedBroadcast) removeBroadcast(cached._2)
    cachedSerializedBroadcast.clear()
  }

  def registerMapOutputs(
    shuffleId: Int,
    statuses: MMap[Int, MapStatus],
    changeEpoch: Boolean = false): Unit = {
    // 这里会替换同shuffleId的非Fake的Stage的map输出
    mapStatuses.put(shuffleId, statuses.clone())
    if (changeEpoch) incrementEpoch()
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    val m = mapStatuses(shuffleId)
    m.synchronized {
      m(mapId) = status
    }
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    val arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      val array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  // TODO-lzp: 感觉下面两个方法有问题，在过滤空块的情况下，某个SiteDriver上可能没有块数据

  // 注册来自bmId的remoteShuffle，partsNum为与shuffleId相关的reduce分区个数
  // 初始化为(false, 0)表示此时未拉取，大小为0
  def registerRemoteShuffle(shuffleId: Int, bmId: BlockManagerId, partsNum: Int): Unit = {
    if (!remoteShuffleFetchStatus.contains(shuffleId)) {
      remoteShuffleFetchStatus(shuffleId) = MMap.empty
    }
    val m = remoteShuffleFetchStatus(shuffleId)
    m.synchronized {
      if (!m.contains(bmId)) {
        m(bmId) = Array.fill(partsNum)(false -> 0)
      }
    }
  }

  def registerRemoteShuffleFetchResult(
    shuffleId: Int, reduceId: Int, blockMId: BlockManagerId, rst: (Boolean, Long)): Unit = {
    if (!remoteShuffleFetchStatus.contains(shuffleId)) {
      logWarning(s"the remote shuffle fetch status doesn't have be register for " +
        s"shuffleId($shuffleId)")
      return
    }
    remoteShuffleFetchStatus(shuffleId)(blockMId)(reduceId) = rst
  }

  def getRemoteShuffleFetchResult(shuffleId: Int): MMap[BlockManagerId, Array[(Boolean, Long)]] =
    remoteShuffleFetchStatus(shuffleId)


  /** Check if the given shuffle is being tracked */
  def containsShuffle(shuffleId: Int): Boolean = mapStatuses.contains(shuffleId)

  // 获取shuffle某个分区的位置偏好，返回主机名列表. 在ShuffledRDD中使用
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
  : Seq[String] = {
    if (shuffleLocalityEnabled && dep.siteMapPartsLen < SHUFFLE_PREF_MAP_THRESHOLD &&
      dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      val blockManagerIds = getLocationsWithLargestOutputs(
        dep.shuffleId, partitionId, REDUCER_PREF_LOCS_FRACTION)
      if (blockManagerIds.nonEmpty) {
        blockManagerIds.get.map(_.host)
      } else {
        Nil
      }
    } else {
      Nil
    }
  }

  // 获取指定shuffleId和reduceId下，分区大小大于阈值的所有BlockManagerId
  def getLocationsWithLargestOutputs(
    shuffleId: Int, reduceId: Int, fractionThreshold: Double): Option[Array[BlockManagerId]] = {
    getLocationsToOutputSize(shuffleId, reduceId).flatMap { case (totalSize, locs) =>
        val topLocs = locs.filter { case (loc, size) =>
            size.toDouble / totalSize >= fractionThreshold
        }
        if (topLocs.isEmpty) None else Some(topLocs.keys.toArray)
    }
  }

  // 获取指定shuffleId/reduceId下，不同BlockManagerId与在此位置的分区总大小的映射
  private def getLocationsToOutputSize(
    shuffleId: Int, reducerId: Int): Option[(Long, Map[BlockManagerId, Long])] = {

    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses != null) {
      statuses.synchronized {
        if (statuses.nonEmpty) {
          // HashMap to add up sizes of all blocks at the same location
          val locs = new SHashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          statuses.values.foreach { status =>
            // status may be null here if we are called between registerShuffle, which creates an
            // array with null entries for each output, and registerMapOutputs, which populates it
            // with valid status entries. This is possible if one thread schedules a job which
            // depends on an RDD which is currently being computed by another thread.
            if (status != null) {
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) {
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                totalOutputSize += blockSize
              }
            }
          }
          return Some((totalOutputSize, locs.toMap))
        }
      }
    }
    None
  }

  def getDataDist(dep: ShuffleDependency[_, _, _], reduceId: Int): Option[Map[String, Long]] =
    getLocationsToOutputSize(dep.shuffleId, reduceId).map(
      _._2.map { case (loc, size) => (loc.host, size)}
    )

  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"spark.shuffle.mapOutput.minSizeForBroadcast ($minSizeForBroadcast bytes) must " +
      s"be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an rpc " +
      "message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  def post(message: GetStatusMessage): Unit = {
    mapOutputRequests.offer(message)
  }

  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputRequests.take()
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            data match {
              case GetMapOutputMessage(shuffleId, context) =>
                val mapOutputStatuses = getSerializedMapOutputStatuses(shuffleId)
                context.reply(mapOutputStatuses)
              case GetRemoteShuffleMessage(requestId, shuffleId, reduceId, context) =>
                val statuses = remoteShuffleFetchStatus(shuffleId)
                var newRequestId = requestId
                if (requestId == -1) {  // 初始化
                  newRequestId = nextRequestId.addAndGet(1)
                  requestIdToBmIds(newRequestId) = mutable.HashSet.empty
                }
                var nonFinished = 0
                val blockIds = statuses.flatMap{ case (bmId, parts) =>
                  val sp = parts(reduceId)
                  if (!sp._1) {  // 未完成
                    nonFinished += 1
                    None
                  } else if (requestIdToBmIds(newRequestId).contains(bmId)) {  // 已传输
                    None
                  } else {  // 未传输
                    requestIdToBmIds(newRequestId) += bmId
                    Some((HostAwareShuffleBlockId(bmId.hostPort, shuffleId, reduceId), sp._2))
                  }
                }.toSeq
                context.reply((newRequestId, sdriverBlockManagerId, blockIds, nonFinished))
            }
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  private val PoisonPill = new GetMapOutputMessage(-99, null)

  private[spark] def getNumCachedSerializedBroadcast: Int = cachedSerializedBroadcast.size

  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: MMap[Int, MapStatus] = null
    var retBytes: Array[Byte] = null
    var epochGotten: Long = -1

    // Check to see if we have a cached version, returns true if it does
    // and has side effect of setting retBytes.  If not returns false
    // with side effect of setting statuses
    def checkCachedStatuses(): Boolean = {
      epochLock.synchronized {
        if (epoch > cacheEpoch) {
          cachedSerializedStatuses.clear()
          clearCachedBroadcast()
          cacheEpoch = epoch
        }
        cachedSerializedStatuses.get(shuffleId) match {
          case Some(bytes) =>
            retBytes = bytes
            true
          case None =>
            logDebug("cached status not found for : " + shuffleId)
            statuses = mapStatuses.getOrElse(shuffleId, MMap.empty[Int, MapStatus])
            epochGotten = epoch
            false
        }
      }
    }

    if (checkCachedStatuses()) return retBytes
    var shuffleIdLock = shuffleIdLocks.get(shuffleId)
    if (null == shuffleIdLock) {
      val newLock = new Object()
      // in general, this condition should be false - but good to be paranoid
      val prevLock = shuffleIdLocks.putIfAbsent(shuffleId, newLock)
      shuffleIdLock = if (null != prevLock) prevLock else newLock
    }
    // synchronize so we only serialize/broadcast it once since multiple threads call
    // in parallel
    shuffleIdLock.synchronized {
      // double check to make sure someone else didn't serialize and cache the same
      // mapstatus while we were waiting on the synchronize
      if (checkCachedStatuses()) return retBytes

      // If we got here, we failed to find the serialized locations in the cache, so we pulled
      // out a snapshot of the locations as "statuses"; let's serialize and return that
      val (bytes, bcast) = MapOutputTracker.serializeMapStatuses(statuses, broadcastManager,
        isLocal, minSizeForBroadcast)
      logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
      // Add them into the table only if the epoch hasn't changed while we were working
      epochLock.synchronized {
        if (epoch == epochGotten) {
          cachedSerializedStatuses(shuffleId) = bytes
          if (null != bcast) cachedSerializedBroadcast(shuffleId) = bcast
        } else {
          logInfo("Epoch changed, not caching!")
          removeBroadcast(bcast)
        }
      }
      bytes
    }
  }

  override def stop() {
    mapOutputRequests.offer(PoisonPill)
    threadpool.shutdown()
    sendTracker(StopMapOutputTracker)
    mapStatuses.clear()
    trackerEndpoint = null
    cachedSerializedStatuses.clear()
    clearCachedBroadcast()
    shuffleIdLocks.clear()
  }
}

private[spark] trait MapOutputTrackerWorkerRole extends MapOutputTracker {
  var masterTrackerEndpoint: RpcEndpointRef = _

  private val fetching = new HashSet[Int]

  // 缓存从Master那拉取的状态
  protected val cachedMapStatuses = new ConcurrentHashMap[Int, MMap[Int, MapStatus]]().asScala

  protected var cachedEpoch: Long = 0

  protected val cachedEpochLock = new AnyRef

  protected def askMasterTracker[T: ClassTag](message: Any): T = {
    try {
      masterTrackerEndpoint.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  protected def sendMasterTracker(message: Any) {
    val response = askMasterTracker[Boolean](message)
    if (!response) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  // 如果本地有则取，否则从Master拉取，并缓存
  def getCachedStatuses(shuffleId: Int): MMap[Int, MapStatus] = {
    val statuses = cachedMapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTime = System.currentTimeMillis
      var fetchedStatuses: MMap[Int, MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        while (fetching.contains(shuffleId)) {
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = cachedMapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the statuses; do so
        logInfo("Doing the fetch; tracker endpoint = " + masterTrackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes = askMasterTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          cachedMapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        return fetchedStatuses
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      return statuses
    }
  }

  // 虽然觉得这个方法名跟它的功能基本不匹配，但是，还是不改的好
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1)
  }

  def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    // 这里其实一次性获取此shuffleId所有的输出，所以，只有第一次调用会慢点, 之后会非常快，因为有缓存
    val statuses = getCachedStatuses(shuffleId)  // 获取跟此shuffleId相关的在集群本地的map输出位置信息
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized {
      return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
    }
  }

  def getRemoteShuffleStatuses(requestId: Long, shuffleId: Int, reduceId: Int)
  : (Long, BlockManagerId, Seq[(BlockId, Long)], Int) = {
    askMasterTracker[(Long, BlockManagerId, Seq[(BlockId, Long)], Int)](
      GetRemoteShuffleStatuses(requestId, shuffleId, reduceId))
  }

  override def unregisterShuffle(shuffleId: Int): Unit = cachedMapStatuses.remove(shuffleId)

  def updateCachedEpoch(newEpoch: Long) {
    cachedEpochLock.synchronized {
      if (newEpoch > cachedEpoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        cachedEpoch = newEpoch
        cachedMapStatuses.clear()
      }
    }
  }

  override def stop(): Unit = {}
}

// 虽然有迷惑性，虽然统一了mapStatuses，但是MOTGM的键为subStageIdx，而MOTM的键为mapId
private[spark] class MapOutputTrackerGlobalMaster(
  override protected val conf: SparkConf,
  override protected val broadcastManager: BroadcastManager,
  override protected val isLocal: Boolean) extends MapOutputTrackerMasterRole

private[spark] class MapOutputTrackerMaster(
  override protected val conf: SparkConf,
  override protected val broadcastManager: BroadcastManager,
  override protected val isLocal: Boolean)
  extends MapOutputTrackerMasterRole with MapOutputTrackerWorkerRole {

  // 注册当前集群的ShuffleMapStage的输出, 注册parts是不连续的
  def registerSiteMapOutputs(
    shuffleId: Int,
    parts: Array[Int],
    statuses: Array[MapStatus],
    changeEpoch: Boolean = false) {
    mapStatuses(shuffleId) = MMap.empty[Int, MapStatus] ++ parts.zip(statuses.clone())
    if (changeEpoch) {
      incrementEpoch()
    }
  }

  override def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
    cachedSerializedStatuses.remove(shuffleId)
    cachedSerializedBroadcast.remove(shuffleId).foreach(v => removeBroadcast(v))
    shuffleIdLocks.remove(shuffleId)
  }
}

private[spark] class MapOutputTrackerWorker extends MapOutputTrackerWorkerRole

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: MMap[Int, MapStatus], broadcastManager: BroadcastManager,
      isLocal: Boolean, minBroadcastSize: Int): (Array[Byte], Broadcast[Array[Byte]]) = {
    val out = new ByteArrayOutputStream
    out.write(DIRECT)
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val arr = out.toByteArray
    if (arr.length >= minBroadcastSize) {
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // toByteArray creates copy, so we can reuse out
      out.reset()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(new GZIPOutputStream(out))
      oos.writeObject(bcast)
      oos.close()
      val outArr = out.toByteArray
      logInfo("Broadcast mapstatuses size = " + outArr.length + ", actual size = " + arr.length)
      (outArr, bcast)
    } else {
      (arr, null)
    }
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): MMap[Int, MapStatus] = {
    assert (bytes.length > 0)

    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val objIn = new ObjectInputStream(new GZIPInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[MMap[Int, MapStatus]]
      case BROADCAST =>
        // deserialize the Broadcast, pull .value array out of it, and then deserialize that
        val bcast = deserializeObject(bytes, 1, bytes.length - 1).
          asInstanceOf[Broadcast[Array[Byte]]]
        logInfo("Broadcast mapstatuses size = " + bytes.length +
          ", actual size = " + bcast.value.length)
        // Important - ignore the DIRECT tag ! Start from offset 1
        deserializeObject(bcast.value, 1, bcast.value.length - 1)
          .asInstanceOf[MMap[Int, MapStatus]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * Given an array of map statuses and a range of map output partitions, returns a sequence that,
   * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
   * stored at that block manager.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param startPartition Start of map output partition ID range (included in range)
   * @param endPartition End of map output partition ID range (excluded from range)
   * @param statuses List of map statuses, indexed by map ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      statuses: MMap[Int, MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new SHashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for ((mapId, status) <- statuses) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
      } else {
        for (part <- startPartition until endPartition) {
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
      }
    }

    splitsByAddress.toSeq
  }
}
