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
package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, MapOutputTrackerMaster, Partition, ShuffleDependency, SparkEnv, TaskContext}

// FakeShuffledRDD的shuffleDep中rdd是不参与序列化的, 这本来就是Spark在shuffle处切断跟上一个Stage联系的设定
// 但是, 因为FakeShuffledRDD的shuffleDep是来自于SD中ShuffleMapStage的shuffleDep, 其经过了从GD/SD的序列化
// 反序列化, 因此, 没有rdd. 这导致很多在dep.rdd的操作失败
class FakeShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
  @transient var prev: RDD[_ <: Product2[K, V]],
  shuffleDep: ShuffleDependency[K, V, C]
) extends RDD[(K, C)](prev.context, Nil) {

  override val partitioner = Option(shuffleDep.partitioner)

  override def getDependencies: Seq[Dependency[_]] = List(shuffleDep)

  override def getPartitions: Array[Partition] =
    Array.tabulate[Partition](shuffleDep.partitioner.numPartitions)(
      i => new ShuffledRDDPartition(i)
    )

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
