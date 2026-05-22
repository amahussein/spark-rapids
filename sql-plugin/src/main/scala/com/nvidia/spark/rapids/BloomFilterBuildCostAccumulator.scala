/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext

/** Driver-side build-cost accumulator keyed by bfId. */
class BloomFilterBuildCostAccumulator
    extends BloomFilterLongPairAccumulator
    with BloomFilterBuildCostUpdater {

  override protected def newEmpty(): BloomFilterBuildCostAccumulator =
    new BloomFilterBuildCostAccumulator

  override def update(buildWallNanos: Long, bfBytes: Long): Unit =
    add((buildWallNanos, bfBytes))
}

object BloomFilterBuildCostAccumulator {

  private val cache = new ConcurrentHashMap[String, BloomFilterBuildCostAccumulator]()

  /** Registers or returns the cached `cubf_build_<bfId>` accumulator. */
  def driverGetOrCreate(sc: SparkContext, bfId: String): BloomFilterBuildCostAccumulator =
    BloomFilterLongPairAccumulator.getOrCreateCached(
      cache, sc, bfId, "cubf_build", () => new BloomFilterBuildCostAccumulator)
}
