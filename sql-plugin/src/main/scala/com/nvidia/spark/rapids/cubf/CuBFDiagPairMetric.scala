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

package com.nvidia.spark.rapids.cubf

import java.util.concurrent.ConcurrentHashMap

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.AccumulatorV2

/**
 * Driver-side cuBF diagnostic metric that accumulates a `(Long, Long)` pair per `bfId`.
 *
 * The pair semantics come from the registered Spark accumulator name:
 *   - `cubf_build_<executionId>_<bfId>` records `(buildWallNanos, bfBytes)`
 *   - `cubf_probe_<executionId>_<bfId>` records `(rowsIn, rowsPassed)`
 *
 * This is observability-only for Spark UI / event logs. Production code does not read the
 * accumulated value.
 */
final class CuBFDiagPairMetric extends AccumulatorV2[(Long, Long), (Long, Long)] {
  private var first: Long = 0L
  private var second: Long = 0L

  override def isZero: Boolean = first == 0L && second == 0L

  override def copy(): CuBFDiagPairMetric = {
    val acc = new CuBFDiagPairMetric
    acc.first = first
    acc.second = second
    acc
  }

  override def reset(): Unit = {
    first = 0L
    second = 0L
  }

  override def add(v: (Long, Long)): Unit = synchronized {
    first += v._1
    second += v._2
  }

  override def merge(other: AccumulatorV2[(Long, Long), (Long, Long)]): Unit = synchronized {
    val o = other.asInstanceOf[CuBFDiagPairMetric]
    first += o.first
    second += o.second
  }

  override def value: (Long, Long) = (first, second)

  def update(a: Long, b: Long): Unit = add((a, b))
}

object CuBFDiagPairMetric {

  private[rapids] case class CacheKey(executionId: Long, bfId: String)

  private val buildCache = new ConcurrentHashMap[CacheKey, CuBFDiagPairMetric]()
  private val probeCache = new ConcurrentHashMap[CacheKey, CuBFDiagPairMetric]()

  private[cubf] final case class RemovedCounts(build: Int, probe: Int) {
    def total: Int = build + probe
  }

  // Diagnostic accumulator keys must carry a real id payload, not just the cuBF prefix.
  private[rapids] def isUsableBfId(bfId: String): Boolean =
    bfId != null && bfId.nonEmpty && bfId != "cubf-"

  /** Registers or returns the cached `cubf_build_<executionId>_<bfId>` metric. */
  def buildForBfId(sc: SparkContext, bfId: String): CuBFDiagPairMetric =
    getOrCreate(buildCache, sc, bfId, "cubf_build")

  /** Registers or returns the cached `cubf_probe_<executionId>_<bfId>` metric. */
  def probeForBfId(sc: SparkContext, bfId: String): CuBFDiagPairMetric =
    getOrCreate(probeCache, sc, bfId, "cubf_probe")

  /**
   * Clears both diagnostic metric caches for one execution.
   *
   * Build and probe metrics remain separate accumulator objects; this helper only centralizes
   * the two-cache lifecycle operation now that both cache values share the same concrete type.
   */
  private[cubf] def removeForExecution(executionId: Long): RemovedCounts =
    RemovedCounts(
      build = removeFromCache(buildCache, executionId),
      probe = removeFromCache(probeCache, executionId))

  private def getOrCreate(
      cache: ConcurrentHashMap[CacheKey, CuBFDiagPairMetric],
      sc: SparkContext,
      bfId: String,
      namePrefix: String): CuBFDiagPairMetric = {
    sqlExecutionId(sc) match {
      case Some(executionId) =>
        val key = CacheKey(executionId, bfId)
        cache.computeIfAbsent(key, _ => register(sc, s"${namePrefix}_${executionId}_$bfId"))
      case None =>
        register(sc, s"${namePrefix}_no_sql_$bfId")
    }
  }

  private def removeFromCache(
      cache: ConcurrentHashMap[CacheKey, CuBFDiagPairMetric],
      executionId: Long): Int = {
    val keys = cache.keySet().iterator()
    var removed = 0
    while (keys.hasNext) {
      val key = keys.next()
      if (key.executionId == executionId && cache.remove(key) != null) {
        removed += 1
      }
    }
    removed
  }

  private def register(sc: SparkContext, name: String): CuBFDiagPairMetric = {
    val acc = new CuBFDiagPairMetric
    sc.register(acc, name)
    acc
  }

  private def sqlExecutionId(sc: SparkContext): Option[Long] = {
    Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .flatMap(id => Try(id.toLong).toOption)
  }

  private[rapids] def clearAllForTests(): Unit = {
    buildCache.clear()
    probeCache.clear()
  }

  private[rapids] def buildCacheSize: Int = buildCache.size()

  private[rapids] def probeCacheSize: Int = probeCache.size()

  private[rapids] def buildContains(executionId: Long, bfId: String): Boolean =
    buildCache.containsKey(CacheKey(executionId, bfId))

  private[rapids] def probeContains(executionId: Long, bfId: String): Boolean =
    probeCache.containsKey(CacheKey(executionId, bfId))
}
