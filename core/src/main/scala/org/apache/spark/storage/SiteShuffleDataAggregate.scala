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

import java.io.{File, IOException}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.util.{ThreadUtils, Utils}

class SiteShuffleDataAggregate(
  shuffleDep: ShuffleDependency[_, _, _],
  conf: SparkConf,
  blockManager: BlockManager,
  shuffleManager: ShuffleManager,
  serInstance: SerializerInstance,
  partIds: Array[Int]
) {

  private val ready: ConcurrentHashMap[Int, (Boolean, Long)] = new ConcurrentHashMap()

  def getReady(partId: Int): (Boolean, Long) = ready.get(partId)

  private val aggregateDataScheduler =
    ThreadUtils.newDaemonCachedThreadPool("aggregate-shuffle-data")

  def aggregateData(dep: ShuffleDependency[_, _, _], partId: Int): Unit = {
    val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
    val taskContext = TaskContext.empty()
    val iter = shuffleManager
      .getReader(dep.shuffleHandle, partId, partId + 1, taskContext)
      .read()
    val blockId = RemoteShuffleBlockId(dep.shuffleId, partId)
    val dataFile = blockManager.diskBlockManager.getFile(blockId)
    if (dataFile.exists()) { dataFile.delete() }
    val tmp = Utils.tempFileWith(dataFile)
    val writer = blockManager.getDiskWriter(
      blockId, tmp, serInstance, fileBufferSize, taskContext.taskMetrics.shuffleWriteMetrics
    )
    while (iter.hasNext) {
      var cur = iter.next()
      writer.write(cur._1, cur._2)
    }
    val segment = writer.commitAndGet()
    if (tmp.exists() && !tmp.renameTo(dataFile)) {
      throw new IOException(s"fail to rename file $tmp to $dataFile")
    }
    ready.put(partId, (true, segment.length))
  }

  def start(): Unit = {
    partIds.foreach { partId =>
      aggregateDataScheduler.submit(new Runnable {
        override def run(): Unit = {
          aggregateData(shuffleDep, partId)
        }
      })
    }
  }

  def stop(): Unit = {
    aggregateDataScheduler.shutdown()
  }
}
