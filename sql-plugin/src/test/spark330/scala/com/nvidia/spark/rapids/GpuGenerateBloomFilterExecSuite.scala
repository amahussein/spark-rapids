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

import com.nvidia.spark.rapids.BloomFilterTestHelpers._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan

/** Tests multi-spec `GpuGenerateBloomFilterExec` driver-side behavior. */
class GpuGenerateBloomFilterExecSuite extends AnyFunSuite
    with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("GpuGenerateBloomFilterExecSuite")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  private def stubChild(): SparkPlan = spark.range(0).queryExecution.executedPlan

  test("multi_spec_registers_N_accumulators") {
    val specs = Seq(
      BFSpec("bf-A", 0, 5, 100000L),
      BFSpec("bf-B", 1, 5, 100000L),
      BFSpec("bf-C", 2, 5, 100000L))
    val exec = GpuGenerateBloomFilterExec(
      specs = specs,
      bfVersion = 1,
      seed = 0,
      xxHashSeed = 42L,
      child = stubChild())
    val accs = exec.accumulators
    assert(accs.size == 3,
      s"expected 3 accumulators, got ${accs.size}")
    assert(accs.keySet == Set("bf-A", "bf-B", "bf-C"),
      s"unexpected bfId set: ${accs.keySet}")
    specs.foreach { spec =>
      val acc = accs(spec.bfId)
      assert(acc.name.contains(s"cuBF-${spec.bfId}"),
        s"accumulator for ${spec.bfId} has name ${acc.name}, " +
          s"expected Some(cuBF-${spec.bfId})")
    }
  }

  test("single_spec_registers_one_accumulator") {
    val spec = BFSpec("single", 0, 7, 524288L)
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(spec),
      bfVersion = 1,
      seed = 0,
      xxHashSeed = 42L,
      child = stubChild())
    val accs = exec.accumulators
    assert(accs.size == 1)
    assert(accs.keySet == Set("single"))
    assert(accs("single").name.contains("cuBF-single"))
  }

  Seq(1, 2).foreach { version =>
    test(s"multi_spec_produces_N_bfs_no_cross_contamination [v$version]") {
      val specs = Seq(
        BFSpec("bf-A", 0, 1, 64),
        BFSpec("bf-B", 1, 1, 64))
      val exec = GpuGenerateBloomFilterExec(
        specs = specs,
        bfVersion = version,
        seed = 0,
        xxHashSeed = 42L,
        child = stubChild())
      val accs = exec.accumulators
      accs("bf-A").add(makeBfBytes(version = version, dataLastByte = 0x0F))
      accs("bf-A").add(makeBfBytes(version = version, dataLastByte = 0xF0))
      accs("bf-B").add(makeBfBytes(version = version, dataLastByte = 0x11))
      accs("bf-B").add(makeBfBytes(version = version, dataLastByte = 0x22))
      val bfA = accs("bf-A").value
      val bfB = accs("bf-B").value
      val dataLastIdx = headerSize(version) + 7
      assert((bfA(dataLastIdx) & 0xFF) == 0xFF,
        s"bf-A data expected 0xFF, got ${bfA(dataLastIdx) & 0xFF}")
      assert((bfB(dataLastIdx) & 0xFF) == 0x33,
        s"bf-B data expected 0x33, got ${bfB(dataLastIdx) & 0xFF}")
      assert(bfA(3) == version && bfB(3) == version, "BF version header corrupted")
    }
  }

  test("canonical_is_always_transparent") {
    val child = stubChild()
    val execSingle = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("single", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L, child = child)
    val execMulti = GpuGenerateBloomFilterExec(
      specs = Seq(
        BFSpec("bf-A", 0, 5, 100000L),
        BFSpec("bf-B", 1, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L, child = child)
    assert(execSingle.canonicalized == child.canonicalized,
      "single-spec GpuGenerateBloomFilterExec must be transparent")
    assert(execMulti.canonicalized == child.canonicalized,
      "multi-spec GpuGenerateBloomFilterExec must be transparent")
    val execMulti2 = GpuGenerateBloomFilterExec(
      specs = Seq(
        BFSpec("bf-A", 0, 5, 100000L),
        BFSpec("bf-B", 1, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L, child = child)
    assert(execMulti.canonicalized == execMulti2.canonicalized,
      "sibling multi-spec wrappers with identical specs must " +
        "canonicalize equally (ReuseExchange precondition)")
  }

  test("accumulator_markSkipped_publishes_sentinel_value") {
    val acc = new BloomFilterBuildAccumulator()
    assert(acc.isZero)
    acc.markSkipped()
    assert(!acc.isZero)
    assert(acc.value eq BloomFilterBuildAccumulator.SkipSentinel,
      "markSkipped must leave the sentinel identity in `value`")
  }

  Seq(1, 2).foreach { version =>
    test(s"accumulator_merge_sentinel_wins_over_real_bf [v$version]") {
      val sentinel = BloomFilterBuildAccumulator.SkipSentinel
      val realBytes = makeBfBytes(version = version, dataLastByte = 0x42)

      val a1 = new BloomFilterBuildAccumulator()
      a1.add(sentinel.clone())
      a1.add(realBytes)
      assert(a1.value eq sentinel,
        "sentinel-then-real must canonicalize to the sentinel identity")

      val a2 = new BloomFilterBuildAccumulator()
      a2.add(realBytes)
      a2.add(sentinel.clone())
      assert(a2.value eq sentinel,
        "real-then-sentinel must end on the sentinel identity")
    }
  }

  test("accumulator_merge_sentinel_x_sentinel_yields_sentinel") {
    val a = new BloomFilterBuildAccumulator()
    a.markSkipped()
    a.add(Array[Byte](0, 0, 0, 0)) // post-serialization content form
    assert(a.value eq BloomFilterBuildAccumulator.SkipSentinel)
  }

  Seq(1, 2).foreach { version =>
    test(s"accumulator_merge_real_x_real_does_not_canonicalize_to_sentinel [v$version]") {
      val a = new BloomFilterBuildAccumulator()
      a.add(makeBfBytes(version = version, dataLastByte = 0x0F))
      a.add(makeBfBytes(version = version, dataLastByte = 0xF0))
      assert(a.value ne BloomFilterBuildAccumulator.SkipSentinel,
        "two real BFs must not canonicalize to the sentinel")
      assert((a.value(headerSize(version) + 7) & 0xFF) == 0xFF,
        "OR-merge expected 0xFF data")
    }
  }

  Seq(1, 2).foreach { version =>
    test(s"accumulator add does not alias the caller's byte array [v$version]") {
      // The accumulator's value must be independent of the caller's array after add returns,
      // otherwise a later mutation of the source buffer (e.g. release of the underlying GPU
      // payload) could silently corrupt the accumulated bloom filter.
      val bytes = makeBfBytes(version = version, dataLastByte = 0xAB)
      val expected = bytes.clone()
      val acc = new BloomFilterBuildAccumulator()
      acc.add(bytes)
      java.util.Arrays.fill(bytes, 0.toByte)
      assert(java.util.Arrays.equals(acc.value, expected),
        "accumulator value must not alias the caller-provided bytes")
      assert(acc.value ne BloomFilterBuildAccumulator.SkipSentinel,
        "post-mutation value must not collapse to the sentinel identity")
    }
  }

  test("long-pair accumulator merge sums both pair components without aliasing the source") {
    // Each task ships its own long-pair accumulator back to the driver, where Spark folds them
    // into the driver-side aggregate via `merge`. The contract is that both pair components add
    // and that the source instance is not observed to change after a merge into a sibling.
    val driverBuild = new BloomFilterBuildCostAccumulator
    val partitionBuild = new BloomFilterBuildCostAccumulator
    driverBuild.update(buildWallNanos = 7L, bfBytes = 11L)
    partitionBuild.update(buildWallNanos = 13L, bfBytes = 17L)
    driverBuild.merge(partitionBuild)
    assert(driverBuild.value == ((20L, 28L)),
      s"merge must sum both components, got ${driverBuild.value}")
    assert(partitionBuild.value == ((13L, 17L)),
      s"merge must not mutate the source accumulator, got ${partitionBuild.value}")

    val driverProbe = new BloomFilterProbeAccumulator
    val partitionProbe = new BloomFilterProbeAccumulator
    driverProbe.update(rowsIn = 100L, rowsPassed = 80L)
    partitionProbe.update(rowsIn = 25L, rowsPassed = 20L)
    driverProbe.merge(partitionProbe)
    assert(driverProbe.value == ((125L, 100L)),
      s"merge must sum both components, got ${driverProbe.value}")
    assert(partitionProbe.value == ((25L, 20L)),
      s"merge must not mutate the source accumulator, got ${partitionProbe.value}")
  }

  test("resolveEffectiveMaxFilterBytes is fail-safe on missing capability helper") {
    val cap = GpuGenerateBloomFilterExec.resolveEffectiveMaxFilterBytes()
    val v1Ceiling = (1L << 31) / 8L
    assert(cap == v1Ceiling,
      s"expected V1 ceiling ($v1Ceiling); got $cap. " +
        "the helper must fail closed to the V1 cap.")
  }

  test("requires_nonEmpty_specs") {
    val ex = intercept[IllegalArgumentException] {
      GpuGenerateBloomFilterExec(
        specs = Seq.empty,
        bfVersion = 1, seed = 0, xxHashSeed = 42L,
        child = stubChild())
    }
    assert(ex.getMessage.contains("at least one"),
      s"unexpected message: ${ex.getMessage}")
  }

  test("recordBuildUpdate invokes updater exactly once per BF build") {
    val spy = new CountingBuildUpdater
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-r7-single", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map("cubf-r7-single" -> spy))
    exec.recordBuildUpdate("cubf-r7-single", 12345678L, 4096L)
    assert(spy.invocationCount === 1,
      "update must fire once per BF build, never per batch or per row")
    assert(spy.lastBuildWallNanos === 12345678L)
    assert(spy.lastBfBytes === 4096L)
  }

  test("multi-BF build invokes each updater exactly once") {
    val spyA = new CountingBuildUpdater
    val spyB = new CountingBuildUpdater
    val spyC = new CountingBuildUpdater
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(
        BFSpec("cubf-r7-A", 0, 5, 100000L),
        BFSpec("cubf-r7-B", 1, 5, 200000L),
        BFSpec("cubf-r7-C", 2, 5, 300000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map(
        "cubf-r7-A" -> spyA,
        "cubf-r7-B" -> spyB,
        "cubf-r7-C" -> spyC))
    exec.recordBuildUpdate("cubf-r7-A", 100L, 1024L)
    exec.recordBuildUpdate("cubf-r7-B", 200L, 2048L)
    exec.recordBuildUpdate("cubf-r7-C", 300L, 4096L)
    assert(spyA.invocationCount === 1)
    assert(spyB.invocationCount === 1)
    assert(spyC.invocationCount === 1)
    assert((spyA.lastBuildWallNanos, spyA.lastBfBytes) === ((100L, 1024L)))
    assert((spyB.lastBuildWallNanos, spyB.lastBfBytes) === ((200L, 2048L)))
    assert((spyC.lastBuildWallNanos, spyC.lastBfBytes) === ((300L, 4096L)))
  }

  test("recordBuildUpdate is a no-op when buildCostUpdaters is empty") {
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-no-updater", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild())
    exec.recordBuildUpdate("cubf-no-updater", 1000000L, 8192L)
    succeed
  }

  test("recordBuildUpdate is a no-op when bfId is not in the map") {
    val spy = new CountingBuildUpdater
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-known", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map("cubf-known" -> spy))
    exec.recordBuildUpdate("cubf-unknown", 1000L, 512L)
    assert(spy.invocationCount === 0,
      "updater must not fire for an unknown bfId")
  }

  test("isNeeded returns false for plan without markers") {
    val plan = spark.range(10).queryExecution.executedPlan
    assert(!InlineBFBuildReplacement.isNeeded(plan),
      "isNeeded must return false when no InlineBFBuildExec markers are present")
  }

  test("applyIfNeeded returns plan unchanged when no markers are present") {
    val plan = spark.range(10).queryExecution.executedPlan
    assert(InlineBFBuildReplacement.applyIfNeeded(plan) eq plan,
      "applyIfNeeded must return the original plan reference unchanged")
  }

  test("buildCostUpdaters do not break canonical transparency") {
    val child = stubChild()
    val spy1 = new CountingBuildUpdater
    val spy2 = new CountingBuildUpdater
    val specs = Seq(
      BFSpec("bf-A", 0, 5, 100000L),
      BFSpec("bf-B", 1, 5, 100000L))
    val with1 = GpuGenerateBloomFilterExec(
      specs = specs, bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = child,
      buildCostUpdaters = Map("bf-A" -> spy1, "bf-B" -> spy1))
    val with2 = GpuGenerateBloomFilterExec(
      specs = specs, bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = child,
      buildCostUpdaters = Map("bf-A" -> spy2, "bf-B" -> spy2))
    val without = GpuGenerateBloomFilterExec(
      specs = specs, bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = child)
    assert(with1.canonicalized == with2.canonicalized,
      "different buildCostUpdaters must canonicalize equal " +
        "for exchange reuse")
    assert(with1.canonicalized == without.canonicalized,
      "presence vs absence of buildCostUpdaters must canonicalize equal")
    assert(with1.canonicalized == child.canonicalized,
      "canonical form drops the wrapper entirely")
  }
}
