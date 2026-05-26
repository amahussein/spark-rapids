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

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.AccumulatorV2

/** AccumulatorV2 base for per-bfId `(Long, Long)` CuBF metrics. */
abstract class BloomFilterLongPairAccumulator
    extends AccumulatorV2[(Long, Long), (Long, Long)] {

  /** Creates a zero-state instance of the concrete accumulator type. */
  protected def newEmpty(): BloomFilterLongPairAccumulator

  private var first: Long = 0L
  private var second: Long = 0L

  override def isZero: Boolean = first == 0L && second == 0L

  override def copy(): AccumulatorV2[(Long, Long), (Long, Long)] = {
    val acc = newEmpty()
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
    val o = other.asInstanceOf[BloomFilterLongPairAccumulator]
    first += o.first
    second += o.second
  }

  override def value: (Long, Long) = (first, second)
}

object BloomFilterLongPairAccumulator {

  private[rapids] case class CuBFDiagAccCacheKey(executionId: Long, bfId: String)

  // Diagnostic accumulator keys must carry a real id payload, not just the cuBF prefix.
  private[rapids] def isUsableBfId(bfId: String): Boolean =
    bfId != null && bfId.nonEmpty && bfId != "cubf-"

  /** Registers or returns a cached named accumulator for the active SQL execution and `bfId`. */
  def getOrCreateCached[A <: BloomFilterLongPairAccumulator](
      cache: ConcurrentHashMap[CuBFDiagAccCacheKey, A],
      sc: SparkContext,
      bfId: String,
      namePrefix: String,
      factory: () => A): A = {
    sqlExecutionId(sc) match {
      case Some(executionId) =>
        val key = CuBFDiagAccCacheKey(executionId, bfId)
        cache.computeIfAbsent(key, _ => register(sc, s"${namePrefix}_${executionId}_$bfId",
          factory))
      case None =>
        register(sc, s"${namePrefix}_no_sql_$bfId", factory)
    }
  }

  def removeForExecution[A <: BloomFilterLongPairAccumulator](
      cache: ConcurrentHashMap[CuBFDiagAccCacheKey, A],
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

  def clearAllForTests[A <: BloomFilterLongPairAccumulator](
      cache: ConcurrentHashMap[CuBFDiagAccCacheKey, A]): Unit = {
    cache.clear()
  }

  private def register[A <: BloomFilterLongPairAccumulator](
      sc: SparkContext,
      name: String,
      factory: () => A): A = {
    val acc = factory()
    sc.register(acc, name)
    acc
  }

  private def sqlExecutionId(sc: SparkContext): Option[Long] = {
    Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .flatMap(id => Try(id.toLong).toOption)
  }
}
