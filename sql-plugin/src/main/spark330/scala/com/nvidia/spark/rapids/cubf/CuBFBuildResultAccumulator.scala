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

import org.apache.spark.internal.Logging
import org.apache.spark.util.AccumulatorV2

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
class CuBFBuildResultAccumulator extends AccumulatorV2[Array[Byte], Array[Byte]]
    with Logging {

  import CuBFBuildResultAccumulator.SkipSentinel

  private var merged: Array[Byte] = _

  override def isZero: Boolean = merged == null

  override def copy(): AccumulatorV2[Array[Byte], Array[Byte]] = {
    val acc = new CuBFBuildResultAccumulator()
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
    val otherAcc = other.asInstanceOf[CuBFBuildResultAccumulator]
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

object CuBFBuildResultAccumulator {
  /** Canonical in-process skip value; deserialized data is recognized by 4-byte zero content. */
  val SkipSentinel: Array[Byte] = Array[Byte](0, 0, 0, 0)
}
