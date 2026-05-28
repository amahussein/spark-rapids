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
package com.nvidia.spark.rapids.cubf

import ai.rapids.cudf.{ColumnView, HostMemoryBuffer, Scalar}
import com.nvidia.spark.rapids.{ExecChecks, ExecRule, GpuColumnVector, GpuExec, GpuOverrides}
import com.nvidia.spark.rapids.{GpuSemaphore, SparkPlanMeta, TypeSig}
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

/** Build specification for one inline bloom filter. */
case class CuBFSpec(
    bfId: String,
    keyColumnIndex: Int,
    numHashes: Int,
    numBits: Long)

/**
 * Pass-through GPU operator that builds inline bloom filters while returning child batches
 * unchanged. Each partition publishes serialized partial BFs to driver accumulators by bfId.
 */
case class GpuGenerateCuBFExec(
    specs: Seq[CuBFSpec],
    bfVersion: Int,
    seed: Int,
    xxHashSeed: Long,
    child: SparkPlan,
    buildCostUpdaters: Map[String, CuBFDiagPairMetric] = Map.empty)
    extends ShimUnaryExecNode with GpuExec with Logging {

  require(specs.nonEmpty,
    "GpuGenerateCuBFExec requires at least one CuBFSpec")

  override def output: Seq[Attribute] = child.output

  // This operator does not change batch sizes; no coalescing needed.
  override val coalesceAfter: Boolean = false

  // Planner-side sibling coalescence makes equal children represent the same logical BF,
  // allowing exchange reuse without mixing distinct bfIds.
  override protected def doCanonicalize(): SparkPlan = child.canonicalized

  /** Registers one build-result accumulator per `bfId` on first driver-side access. */
  @transient lazy val accumulators: Map[String, CuBFBuildResultAccumulator] = {
    val sc = sparkContext
    specs.map { spec =>
      val acc = new CuBFBuildResultAccumulator()
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
    val effMaxBytes = GpuGenerateCuBFExec.resolveEffectiveMaxFilterBytes()
    val oversizedBfIds: Set[String] = specsCapture.flatMap { spec =>
      val bfBytes = GpuGenerateCuBFExec.bytesForBits(spec.numBits)
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
              accMap(specsCapture(i).bfId).add(CuBFBuildResultAccumulator.SkipSentinel)
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
                  val bytes = GpuGenerateCuBFExec.scalarToHostBytes(bf)
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
      "GpuGenerateCuBFExec does not support row-based execution")
  }

  /** Records one metrics update for a finalized BF build. */
  private[rapids] def recordBuildUpdate(
      bfId: String, buildWallNanos: Long, bfBytes: Long): Unit = {
    buildCostUpdaters.get(bfId).foreach(_.update(buildWallNanos, bfBytes))
  }
}

object GpuGenerateCuBFExec extends Logging {

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
object InlineCuBFBuildGpuOverride {
  val execRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GpuGenerateCuBFExec](
      "Pass-through GPU operator that builds a bloom filter inline " +
        "with the join's build-side pipeline",
      ExecChecks(TypeSig.all, TypeSig.all),
      (exec, conf, parent, rule) =>
        new SparkPlanMeta[GpuGenerateCuBFExec](exec, conf, parent, rule) {
          override def tagPlanForGpu(): Unit = {}
          override def convertToGpu(): GpuExec =
            exec.copy(child = childPlans.head.convertIfNeeded())
        }
    ).invisible()
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
}
