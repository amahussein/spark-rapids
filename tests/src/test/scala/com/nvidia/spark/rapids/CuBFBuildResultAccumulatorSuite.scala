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

import ai.rapids.cudf.{ColumnVector, ColumnView}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.cubf.{CuBFBuildResultAccumulator, CuBFSpec, GpuGenerateCuBFExec}
import com.nvidia.spark.rapids.jni.{BloomFilter, Hash}

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.LongType

/**
 * Drives a real GPU plan end-to-end so the build-side accumulator value Spark eventually
 * collects matches an independently built bloom filter byte-for-byte.
 */
class CuBFBuildResultAccumulatorSuite extends SparkQueryCompareTestSuite {

  private val NumRows = 1024
  private val NumHashes = 5
  private val NumBits = 1L << 14
  private val XxHashSeed = 42L
  private val BfVersion = 1
  private val BfSeed = 0

  private def newRangeExec(targetSizeBytes: Long): GpuRangeExec =
    GpuRangeExec(
      start = 0L,
      end = NumRows.toLong,
      step = 1L,
      numSlices = 1,
      output = Seq(AttributeReference("id", LongType)()),
      targetSizeBytes = targetSizeBytes)

  private def newBuildExec(bfId: String, rangeExec: GpuRangeExec): GpuGenerateCuBFExec =
    GpuGenerateCuBFExec(
      specs = Seq(CuBFSpec(bfId, keyColumnIndex = 0, numHashes = NumHashes,
        numBits = NumBits)),
      bfVersion = BfVersion,
      seed = BfSeed,
      xxHashSeed = XxHashSeed,
      child = rangeExec)

  test("build-side accumulator matches an independently built reference filter") {
    // Drives a real GPU plan end-to-end so the test exercises Spark's actual
    // task-completion -> accumulator-collection ordering, including serialization.
    val bfId = "cubf-accum-suite"

    val (accValue, expected) = withGpuSparkSession { _ =>
      val rangeExec = newRangeExec(targetSizeBytes = Math.max(NumRows / 8, 1).toLong)
      val exec = newBuildExec(bfId, rangeExec)
      // Collect a full count to drain the iterator chain so Spark fires the completion
      // listeners and collects the accumulator into the DirectTaskResult.
      val produced = exec.executeColumnar().mapPartitions { iter =>
        var count = 0L
        while (iter.hasNext) {
          val b = iter.next()
          count += b.numRows()
          b.close()
        }
        Iterator.single(count)
      }.collect().sum
      assert(produced == NumRows,
        s"expected $NumRows rows to flow through GpuGenerateCuBFExec, got $produced")
      val accBytes = exec.accumulators(bfId).value
      val reference = referenceBfBytes(NumRows, NumHashes, NumBits, BfVersion, BfSeed,
        XxHashSeed)
      (accBytes, reference)
    }

    assert(accValue != null, "build-side accumulator must hold a value after a full drain")
    assert(accValue ne CuBFBuildResultAccumulator.SkipSentinel,
      "fully drained partition must not publish the skip sentinel")
    assert(accValue.length == expected.length,
      s"BF byte length mismatch: ${accValue.length} vs ${expected.length}")
    assert(java.util.Arrays.equals(accValue, expected),
      "accumulator bytes must match an independently built reference bloom filter")
  }

  test("partial drain through executeColumnar publishes the skip sentinel") {
    // Round-trip the early-close path through Spark's real task lifecycle: the second
    // batch's keys are unreachable when each partition takes only the first batch, so the
    // operator's completion listener must emit the sentinel and Spark must merge it into
    // the driver-side accumulator value.
    val bfId = "cubf-accum-partial-drain"

    val accValue = withGpuSparkSession { _ =>
      // targetSizeBytes / 8 = max rows per batch; pick a size that forces ≥ 2 batches with
      // the second batch carrying keys absent from the first.
      val rangeExec = newRangeExec(targetSizeBytes = (NumRows / 4) * 8L)
      val exec = newBuildExec(bfId, rangeExec)
      val collected = exec.executeColumnar().mapPartitions { iter =>
        iter.take(1).map { b =>
          val n = b.numRows()
          b.close()
          n
        }
      }.collect()
      // Two assertions pin the partial-drain *shape*: at least one batch emerged, and
      // strictly fewer rows than the partition contained — otherwise the fixture
      // collapsed to a single full batch and the test would pass even if the listener
      // never emitted the sentinel.
      assert(collected.nonEmpty,
        "partition must emit at least one batch before being abandoned")
      assert(collected.sum < NumRows,
        s"partial-drain fixture degenerated to a full drain: ${collected.sum} of $NumRows " +
          s"rows reached the consumer, so the abandoned-second-batch scenario was never " +
          s"exercised. Adjust targetSizeBytes so the partition produces multiple batches.")
      exec.accumulators(bfId).value
    }

    assert(accValue eq CuBFBuildResultAccumulator.SkipSentinel,
      "partial-drain partition must publish the skip sentinel into the driver-side " +
        s"accumulator after Spark merges the executor result; got " +
        s"${if (accValue == null) "null" else accValue.toSeq}")
  }

  /** Builds the same bloom filter the operator would by feeding the keys through cuDF JNI. */
  private def referenceBfBytes(
      numRows: Int, numHashes: Int, numBits: Long, bfVersion: Int, bfSeed: Int,
      xxHashSeed: Long): Array[Byte] = {
    val keys = (0L until numRows.toLong).toArray
    withResource(ColumnVector.fromLongs(keys: _*)) { keyCol =>
      withResource(Hash.xxhash64(xxHashSeed, Array[ColumnView](keyCol))) { hashedCol =>
        withResource(BloomFilter.create(bfVersion, numHashes, numBits, bfSeed)) { bf =>
          BloomFilter.put(bf, hashedCol)
          GpuGenerateCuBFExec.scalarToHostBytes(bf)
        }
      }
    }
  }
}
