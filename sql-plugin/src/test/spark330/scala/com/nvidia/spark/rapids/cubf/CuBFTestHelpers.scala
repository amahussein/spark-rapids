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

/**
 * Shared CuBF test fixtures.
 *
 * BF wire format is big-endian; V2 adds seed between numHashes and numWords.
 */
object CuBFTestHelpers {

  val V1_HEADER_SIZE: Int = 12

  val V2_HEADER_SIZE: Int = 16

  def headerSize(version: Int): Int =
    if (version == 2) V2_HEADER_SIZE else V1_HEADER_SIZE

  /** Builds a synthetic serialized bloom-filter payload. */
  def makeBfBytes(
      version: Int = 1,
      numHashes: Int = 1,
      numWords: Int = 1,
      dataLastByte: Int = 0,
      seed: Int = 0): Array[Byte] = {
    val header = headerSize(version)
    val dataLen = numWords * 8
    val out = new Array[Byte](header + dataLen)
    writeIntBE(out, 0, version)
    writeIntBE(out, 4, numHashes)
    if (version == 2) {
      writeIntBE(out, 8, seed)
      writeIntBE(out, 12, numWords)
    } else {
      writeIntBE(out, 8, numWords)
    }
    if (dataLen > 0) {
      out(header + dataLen - 1) = dataLastByte.toByte
    }
    out
  }

  private def writeIntBE(buf: Array[Byte], offset: Int, value: Int): Unit = {
    buf(offset)     = ((value >>> 24) & 0xFF).toByte
    buf(offset + 1) = ((value >>> 16) & 0xFF).toByte
    buf(offset + 2) = ((value >>>  8) & 0xFF).toByte
    buf(offset + 3) = ( value         & 0xFF).toByte
  }
}

/** Reflection-target shapes for `InlineCuBFBuildReplacement.readSpecs`. */
object FakeInlineExecs {

  case class FakeSpec(
      bfId: String,
      keyColumnIndex: Int,
      numHashes: Int,
      numBits: Long)

  case class FakeMultiSpecInlineExec(
      specs: Seq[FakeSpec],
      bfVersion: Int,
      seed: Int,
      xxHashSeed: Long,
      child: Any)

  case class FakeLegacyInlineExec(
      bfId: String,
      keyColumnIndex: Int,
      numHashes: Int,
      numBits: Long,
      bfVersion: Int,
      seed: Int,
      xxHashSeed: Long,
      child: Any)

  /** Drives the reflective spec-access failure path in readSpecs. */
  case class FakeBrokenInlineExec(child: Any) {
    def specs: Seq[Any] = throw new RuntimeException("synthetic reflection failure")
    def bfVersion: Int = 1
    def seed: Int = 0
    def xxHashSeed: Long = 42L
  }
}
