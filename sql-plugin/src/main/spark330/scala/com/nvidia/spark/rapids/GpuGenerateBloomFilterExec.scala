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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnView, HostMemoryBuffer, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{BloomFilter, Hash}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.AccumulatorV2

/** Build specification for one inline bloom filter. */
case class BFSpec(
    bfId: String,
    keyColumnIndex: Int,
    numHashes: Int,
    numBits: Long)

/**
 * Pass-through GPU operator that builds inline bloom filters while returning child batches
 * unchanged. Each partition publishes serialized partial BFs to driver accumulators by bfId.
 */
case class GpuGenerateBloomFilterExec(
    specs: Seq[BFSpec],
    bfVersion: Int,
    seed: Int,
    xxHashSeed: Long,
    child: SparkPlan,
    buildCostUpdaters: Map[String, BloomFilterBuildCostUpdater] = Map.empty)
    extends ShimUnaryExecNode with GpuExec with Logging {

  require(specs.nonEmpty,
    "GpuGenerateBloomFilterExec requires at least one BFSpec")

  override def output: Seq[Attribute] = child.output

  // This operator does not change batch sizes; no coalescing needed.
  override val coalesceAfter: Boolean = false

  // Planner-side sibling coalescence makes equal children represent the same logical BF,
  // allowing exchange reuse without mixing distinct bfIds.
  override protected def doCanonicalize(): SparkPlan = child.canonicalized

  /** Registers one build-result accumulator per `bfId` on first driver-side access. */
  @transient lazy val accumulators: Map[String, BloomFilterBuildAccumulator] = {
    val sc = sparkContext
    specs.map { spec =>
      val acc = new BloomFilterBuildAccumulator()
      sc.register(acc, s"cuBF-${spec.bfId}")
      (spec.bfId, acc)
    }.toMap
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // Capture fields for serialization (avoid capturing 'this').
    val specsCapture = specs
    val bfVer = bfVersion
    val bfSeed = seed
    val hashSeed = xxHashSeed
    val accMap = accumulators // triggers eager driver-side registration
    val updatersCapture = buildCostUpdaters

    // Mark oversized specs as skipped before tasks launch.
    val effMaxBytes = GpuGenerateBloomFilterExec.resolveEffectiveMaxFilterBytes()
    val oversizedBfIds: Set[String] = specsCapture.flatMap { spec =>
      val bfBytes = GpuGenerateBloomFilterExec.bytesForBits(spec.numBits)
      if (bfBytes > effMaxBytes) {
        logWarning(s"[CuBF-GpuGenerate] OVERSHOOT bfId=${spec.bfId} " +
          s"bfBytes=$bfBytes > max=$effMaxBytes -> SKIP")
        accMap(spec.bfId).markSkipped()
        Some(spec.bfId)
      } else None
    }.toSet

    val oversizedCapture = oversizedBfIds
    child.executeColumnar().mapPartitions { iter =>
      val ctx = TaskContext.get()
      val partId = ctx.partitionId()
      GpuSemaphore.acquireIfNecessary(ctx)
      val numSpecs = specsCapture.size
      val bfs: Array[Scalar] = new Array[Scalar](numSpecs)
      val rowsProcessed: Array[Long] = Array.fill(numSpecs)(0L)
      val skipSpec: Array[Boolean] = specsCapture
        .map(s => oversizedCapture.contains(s.bfId)).toArray
      @volatile var finalized = false
      // Build-cost timer: first non-empty batch through finalize.
      var taskStartNanos: Long = 0L

      def closeAllBfs(): Unit = {
        var i = 0
        while (i < bfs.length) {
          val bf = bfs(i)
          if (bf != null) {
            try bf.close() catch {
              case e: Throwable =>
                logWarning(s"[CuBF-GpuBuild] bfId=" +
                  s"${specsCapture(i).bfId} partition=$partId " +
                  s"close failed: ${e.getMessage}")
            }
            bfs(i) = null
          }
          i += 1
        }
      }

      ctx.addTaskCompletionListener[Unit] { _ =>
        // Iterator was not drained to exhaustion, so `finalizeAllBFs` never ran for this
        // partition. Without an explicit signal the merged accumulator would silently miss
        // the unconsumed keys, and the BF would produce false negatives against the join.
        // Emit the skip sentinel so the merged BF fails closed instead. Failed / killed /
        // speculation-loser results are dropped before the driver merges them — failed tasks
        // are filtered by `countFailedValues = false`, and speculation losers' results are
        // discarded by the driver-side dedup against the winning attempt — so this emit is
        // local-only on those paths and only reaches the driver when the partition's result
        // is actually accepted.
        if (!finalized) {
          var i = 0
          while (i < numSpecs) {
            if (!skipSpec(i)) {
              accMap(specsCapture(i).bfId).add(BloomFilterBuildAccumulator.SkipSentinel)
            }
            i += 1
          }
        }
        closeAllBfs()
      }

      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val has = iter.hasNext
          if (!has && !finalized) {
            finalized = true
            finalizeAllBFs()
          }
          has
        }

        override def next(): ColumnarBatch = {
          val batch = iter.next()
          if (batch.numRows() > 0) {
            if (taskStartNanos == 0L) {
              taskStartNanos = System.nanoTime()
            }
            var i = 0
            while (i < numSpecs) {
              val spec = specsCapture(i)
              if (!skipSpec(i) && batch.numCols() > spec.keyColumnIndex) {
                if (bfs(i) == null) {
                  bfs(i) = BloomFilter.create(bfVer,
                    spec.numHashes, spec.numBits, bfSeed)
                }
                val keyCol = batch.column(spec.keyColumnIndex)
                  .asInstanceOf[GpuColumnVector].getBase
                // Match the probe-side XxHash64 input to BloomFilterMightContain.
                withResource(Hash.xxhash64(hashSeed,
                    Array[ColumnView](keyCol))) { hashedCol =>
                  BloomFilter.put(bfs(i), hashedCol)
                }
                rowsProcessed(i) += batch.numRows()
              }
              i += 1
            }
          }
          batch // pass through unchanged
        }

        private def finalizeAllBFs(): Unit = {
          try {
            // One fused pass over all specs; per-BF attribution must normalize by spec count.
            val wallNanos = if (taskStartNanos != 0L) {
              System.nanoTime() - taskStartNanos
            } else 0L
            var i = 0
            while (i < numSpecs) {
              val spec = specsCapture(i)
              if (skipSpec(i)) {
                // Oversized spec already marked skipped on the driver.
                logInfo(s"[CuBF-GpuBuild] bfId=${spec.bfId} " +
                  s"partition=$partId SKIPPED (overshoot)")
              } else {
                val bf = bfs(i)
                if (bf != null) {
                  val bytes = GpuGenerateBloomFilterExec.scalarToHostBytes(bf)
                  // `bytes` is a fresh JVM byte array unrelated to any device buffer, and the
                  // accumulator copies the payload into its own heap state (cloning on first
                  // write or OR-merging into an existing buffer). Once `add` returns, releasing
                  // the source `Scalar` in `closeAllBfs()` below cannot disturb the result.
                  accMap(spec.bfId).add(bytes)
                  logInfo(s"[CuBF-GpuBuild] bfId=${spec.bfId} " +
                    s"partition=$partId SENT ${bytes.length} bytes " +
                    s"(${rowsProcessed(i)} rows)")
                  // Skipped and empty partitions do not contribute build cost.
                  updatersCapture.get(spec.bfId).foreach { u =>
                    u.update(wallNanos, bytes.length.toLong)
                  }
                } else {
                  logWarning(s"[CuBF-GpuBuild] bfId=${spec.bfId} " +
                    s"partition=$partId EMPTY " +
                    s"(0 rows processed, no BF constructed)")
                }
              }
              i += 1
            }
          } finally {
            closeAllBfs()
          }
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuGenerateBloomFilterExec does not support row-based execution")
  }

  /** Records one metrics update for a finalized BF build. */
  private[rapids] def recordBuildUpdate(
      bfId: String, buildWallNanos: Long, bfBytes: Long): Unit = {
    buildCostUpdaters.get(bfId).foreach(_.update(buildWallNanos, bfBytes))
  }
}

object GpuGenerateBloomFilterExec extends Logging {

  private val SparkVersionBFCapsClassName =
    "com.nvidia.spark.rapids.optimizer.cubloomfilter.SparkVersionBFCaps$"

  /** V1 BloomFilterImpl indexing ceiling, used when planner caps are unavailable. */
  private val V1IndexingCeilingBytes: Long = (1L << 31) / 8L

  /** Ceil-divide bits to bytes, saturating on overflow so oversized filters fail closed. */
  private def bytesForBits(numBits: Long): Long =
    if (numBits > Long.MaxValue - 7L) Long.MaxValue else (numBits + 7L) / 8L

  // Resolve the planner-provided BF byte cap; fallback keeps overshoot checks fail-closed.
  def resolveEffectiveMaxFilterBytes(): Long = {
    try {
      val cls = Class.forName(SparkVersionBFCapsClassName)
      val module = cls.getField("MODULE$").get(null)
      val method = cls.getMethod("effectiveMaxFilterBytes", classOf[Long])
      method.invoke(module, java.lang.Long.valueOf(Long.MaxValue))
        .asInstanceOf[java.lang.Long].longValue()
    } catch {
      case e: Throwable =>
        logWarning(s"[CuBF-GpuGenerate] Could not resolve " +
          s"SparkVersionBFCaps.effectiveMaxFilterBytes via reflection: " +
          s"${e.getMessage}. Falling back to V1 ceiling " +
          s"($V1IndexingCeilingBytes bytes).")
        V1IndexingCeilingBytes
    }
  }

  /** Copies a serialized GPU bloom-filter scalar to host bytes. */
  def scalarToHostBytes(bf: Scalar): Array[Byte] = {
    withResource(bf.getListAsColumnView) { view =>
      val devBuf = view.getData
      val len = devBuf.getLength.toInt
      withResource(HostMemoryBuffer.allocate(len)) { hostBuf =>
        hostBuf.copyFromDeviceBuffer(devBuf)
        val bytes = new Array[Byte](len)
        hostBuf.getBytes(bytes, 0, 0, len)
        bytes
      }
    }
  }
}

/** Registers the already-GPU inline BF build so GpuOverrides still converts its child. */
object InlineBFBuildGpuOverride {
  val execRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GpuGenerateBloomFilterExec](
      "Pass-through GPU operator that builds a bloom filter inline " +
        "with the join's build-side pipeline",
      ExecChecks(TypeSig.all, TypeSig.all),
      (exec, conf, parent, rule) =>
        new SparkPlanMeta[GpuGenerateBloomFilterExec](exec, conf, parent, rule) {
          override def tagPlanForGpu(): Unit = {}
          override def convertToGpu(): GpuExec =
            exec.copy(child = childPlans.head.convertIfNeeded())
        }
    ).invisible()
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
}

/**
 * Accumulates partial bloom filters by OR-ing their serialized data bytes.
 *
 * A 4-byte all-zero value is the skip sentinel; once seen, the accumulator stays skipped.
 * Real bloom filters cannot collide because their serialized header starts with a non-zero version.
 *
 * Threading: a given instance is mutated by a single executor thread per task (Spark hands each
 * task a freshly deserialized copy), and driver-side merges of those copies are serialized by
 * Spark's accumulator-result handling, so `add`/`merge` do not need internal synchronization.
 */
class BloomFilterBuildAccumulator extends AccumulatorV2[Array[Byte], Array[Byte]]
    with Logging {

  import BloomFilterBuildAccumulator.SkipSentinel

  private var merged: Array[Byte] = _

  override def isZero: Boolean = merged == null

  override def copy(): AccumulatorV2[Array[Byte], Array[Byte]] = {
    val acc = new BloomFilterBuildAccumulator()
    if (merged != null) {
      // Preserve sentinel identity for driver-local fast checks.
      acc.merged = if (merged eq SkipSentinel) SkipSentinel else merged.clone()
    }
    acc
  }

  override def reset(): Unit = {
    merged = null
  }

  /** Publishes the skip sentinel into this accumulator. */
  def markSkipped(): Unit = {
    merged = SkipSentinel
  }

  override def add(partial: Array[Byte]): Unit = {
    // Null partials are ignored; skip is represented by SkipSentinel.
    if (partial == null) {
      return
    }
    if (isSkipShape(partial) || isSkipShape(merged)) {
      merged = SkipSentinel
    } else if (merged == null) {
      merged = partial.clone()
    } else {
      mergeBytes(partial)
    }
  }

  override def merge(other: AccumulatorV2[Array[Byte], Array[Byte]]): Unit = {
    val otherAcc = other.asInstanceOf[BloomFilterBuildAccumulator]
    if (otherAcc.merged != null) {
      add(otherAcc.merged)
    }
  }

  override def value: Array[Byte] = merged

  /** Checks the skip sentinel by identity or serialized content. */
  private def isSkipShape(bytes: Array[Byte]): Boolean = {
    if (bytes == null) false
    else if (bytes eq SkipSentinel) true
    else bytes.length == 4 &&
      bytes(0) == 0.toByte && bytes(1) == 0.toByte &&
      bytes(2) == 0.toByte && bytes(3) == 0.toByte
  }

  /**
   * OR `other` into `merged`, skipping the serialized BF header.
   * Length mismatch means inconsistent BF specs, so fail closed with SkipSentinel.
   */
  private def mergeBytes(other: Array[Byte]): Unit = {
    if (merged.length != other.length) {
      logWarning(s"[CuBF-Accumulator] BF size mismatch: ${merged.length} vs " +
        s"${other.length} -> SKIP")
      merged = SkipSentinel
      return
    }
    // Read version to choose header length.
    val version = ((merged(0) & 0xFF) << 24) | ((merged(1) & 0xFF) << 16) |
      ((merged(2) & 0xFF) << 8) | (merged(3) & 0xFF)
    val dataOffset = if (version == 2) 16 else 12
    var i = dataOffset
    while (i < merged.length) {
      merged(i) = (merged(i) | other(i)).toByte
      i += 1
    }
  }
}

object BloomFilterBuildAccumulator {
  /** Canonical in-process skip value; deserialized data is recognized by 4-byte zero content. */
  val SkipSentinel: Array[Byte] = Array[Byte](0, 0, 0, 0)
}
