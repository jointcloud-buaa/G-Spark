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
package org.apache.spark.scheduler

import scala.annotation.meta.param

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

private[spark] class FakeStage(
  id: Int,
  rdd: RDD[_],  // 这里是由shuffleDep生成的ShuffledRDD
  numTasks: Int,
  @(transient @param) parents: List[Stage] = Nil,
  @(transient @param) firstJobId: Int,  // 继承父Stage
  callSite: CallSite,  // 继承父Stage的吧, 因为FakeStage本身就表示无
  val shuffleDep: ShuffleDependency[_, _, _]
) extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  override def toString: String = "FakeStage " + id

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (idx <- calcPartIds.indices) {
      val prevList = partResults(idx)
      val newList = prevList.filterNot(_.asInstanceOf[MapStatus].location.executorId == execId)
      partResults(idx) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numFinished -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numFinished, calcPartIds.length, isSiteAvailable))
    }
  }
}
