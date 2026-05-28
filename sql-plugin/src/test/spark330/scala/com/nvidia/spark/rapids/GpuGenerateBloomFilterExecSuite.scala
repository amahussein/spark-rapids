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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.nvidia.spark.rapids.BloomFilterTestHelpers._
import com.nvidia.spark.rapids.cubf.CuBFDiagPairMetric
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.SparkPlan

/** Tests multi-spec `GpuGenerateBloomFilterExec` driver-side behavior. */
class GpuGenerateBloomFilterExecSuite extends AnyFunSuite
    with CuBFLocalSparkSuite {

  private def stubChild(): SparkPlan = spark.range(0).queryExecution.executedPlan

  test("multi-spec registers N accumulators") {
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

  test("single-spec registers one accumulator") {
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
    test(s"multi-spec produces N BFs with no cross-contamination [v$version]") {
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

  test("canonical is always transparent") {
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

  test("accumulator markSkipped publishes sentinel value") {
    val acc = new BloomFilterBuildAccumulator()
    assert(acc.isZero)
    acc.markSkipped()
    assert(!acc.isZero)
    assert(acc.value eq BloomFilterBuildAccumulator.SkipSentinel,
      "markSkipped must leave the sentinel identity in `value`")
  }

  Seq(1, 2).foreach { version =>
    test(s"accumulator merge sentinel wins over real BF [v$version]") {
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

  test("accumulator merge sentinel with sentinel yields sentinel") {
    val a = new BloomFilterBuildAccumulator()
    a.markSkipped()
    a.add(Array[Byte](0, 0, 0, 0)) // post-serialization content form
    assert(a.value eq BloomFilterBuildAccumulator.SkipSentinel)
  }

  Seq(1, 2).foreach { version =>
    test(s"accumulator merge real with real does not canonicalize to sentinel [v$version]") {
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
    val driverMetric = new CuBFDiagPairMetric
    val partitionMetric = new CuBFDiagPairMetric
    driverMetric.update(7L, 11L)
    partitionMetric.update(13L, 17L)
    driverMetric.merge(partitionMetric)
    assert(driverMetric.value == ((20L, 28L)),
      s"merge must sum both components, got ${driverMetric.value}")
    assert(partitionMetric.value == ((13L, 17L)),
      s"merge must not mutate the source accumulator, got ${partitionMetric.value}")
  }

  test("build diagnostic updaters require diagnostic config and non-empty bfIds") {
    CuBFDiagPairMetric.clearAllForTests()
    try {
      withSqlConf(RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.key -> "false") {
        withSqlExecutionId(301L) {
          val updaters = InlineBFBuildReplacement()
            .resolveBuildCostUpdaters(Seq("bf-diag-off"))
          assert(updaters.isEmpty)
          assert(CuBFDiagPairMetric.buildCacheSize === 0)
        }
      }

      withSqlConf(RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.key -> "true") {
        withSqlExecutionId(302L) {
          val updaters = InlineBFBuildReplacement()
            .resolveBuildCostUpdaters(Seq("", "cubf-", "bf-diag-on"))
          assert(updaters.keySet === Set("bf-diag-on"))
          assert(CuBFDiagPairMetric.buildContains(302L, "bf-diag-on"))
          assert(!CuBFDiagPairMetric.buildContains(302L, ""))
          assert(!CuBFDiagPairMetric.buildContains(302L, "cubf-"))
        }
        withSqlExecutionId(303L) {
          val updaters = InlineBFBuildReplacement().resolveBuildCostUpdaters(Seq.empty)
          assert(updaters.isEmpty)
          assert(!CuBFDiagPairMetric.buildContains(303L, ""))
        }
      }
    } finally {
      CuBFDiagPairMetric.clearAllForTests()
    }
  }

  test("resolveEffectiveMaxFilterBytes is fail-safe on missing capability helper") {
    val cap = GpuGenerateBloomFilterExec.resolveEffectiveMaxFilterBytes()
    val v1Ceiling = (1L << 31) / 8L
    assert(cap == v1Ceiling,
      s"expected V1 ceiling ($v1Ceiling); got $cap. " +
        "the helper must fail closed to the V1 cap.")
  }

  test("requires nonEmpty specs") {
    val ex = intercept[IllegalArgumentException] {
      GpuGenerateBloomFilterExec(
        specs = Seq.empty,
        bfVersion = 1, seed = 0, xxHashSeed = 42L,
        child = stubChild())
    }
    assert(ex.getMessage.contains("at least one"),
      s"unexpected message: ${ex.getMessage}")
  }

  test("recordBuildUpdate records one pair per BF build") {
    val metric = new CuBFDiagPairMetric
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-r7-single", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map("cubf-r7-single" -> metric))
    exec.recordBuildUpdate("cubf-r7-single", 12345678L, 4096L)
    assert(metric.value === ((12345678L, 4096L)),
      "metric must record one BF build update, never per batch or per row")
  }

  test("multi-BF build records each metric independently") {
    val metricA = new CuBFDiagPairMetric
    val metricB = new CuBFDiagPairMetric
    val metricC = new CuBFDiagPairMetric
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(
        BFSpec("cubf-r7-A", 0, 5, 100000L),
        BFSpec("cubf-r7-B", 1, 5, 200000L),
        BFSpec("cubf-r7-C", 2, 5, 300000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map(
        "cubf-r7-A" -> metricA,
        "cubf-r7-B" -> metricB,
        "cubf-r7-C" -> metricC))
    exec.recordBuildUpdate("cubf-r7-A", 100L, 1024L)
    exec.recordBuildUpdate("cubf-r7-B", 200L, 2048L)
    exec.recordBuildUpdate("cubf-r7-C", 300L, 4096L)
    assert(metricA.value === ((100L, 1024L)))
    assert(metricB.value === ((200L, 2048L)))
    assert(metricC.value === ((300L, 4096L)))
  }

  test("recordBuildUpdate is a no-op when buildCostUpdaters is empty") {
    // A metric wired to a sibling exec must not see invocations from the
    // empty-map exec. Catches Map.get -> Map.apply refactors and any
    // future cross-instance side-effect leak.
    val metric = new CuBFDiagPairMetric
    val sibling = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-active", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map("cubf-active" -> metric))
    val target = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-no-updater", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild())
    target.recordBuildUpdate("cubf-no-updater", 1000000L, 8192L)
    assert(metric.value === ((0L, 0L)),
      "empty buildCostUpdaters path must not fire any other exec's metric")
    // Sanity check: the metric fires for its own owner.
    sibling.recordBuildUpdate("cubf-active", 1L, 1L)
    assert(metric.value === ((1L, 1L)))
  }

  test("recordBuildUpdate is a no-op when bfId is not in the map") {
    val metric = new CuBFDiagPairMetric
    val exec = GpuGenerateBloomFilterExec(
      specs = Seq(BFSpec("cubf-known", 0, 5, 100000L)),
      bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = stubChild(),
      buildCostUpdaters = Map("cubf-known" -> metric))
    exec.recordBuildUpdate("cubf-unknown", 1000L, 512L)
    assert(metric.value === ((0L, 0L)),
      "metric must not update for an unknown bfId")
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

  Seq(1, 2).foreach { version =>
    test(s"driver-side merge of one real and one sentinel partition yields sentinel [v$version]") {
      // Each partition ships its own accumulator copy back to the driver. If one partition
      // emits the skip sentinel because it closed early, the driver-side merge must promote
      // the merged value to the sentinel so a probe filter built from a strict subset of
      // build keys cannot silently drop matching probe rows.
      val real = new BloomFilterBuildAccumulator()
      real.add(makeBfBytes(version = version, dataLastByte = 0x42))
      val poisoned = new BloomFilterBuildAccumulator()
      poisoned.add(BloomFilterBuildAccumulator.SkipSentinel)
      val driver = new BloomFilterBuildAccumulator()
      driver.merge(real)
      driver.merge(poisoned)
      assert(driver.value eq BloomFilterBuildAccumulator.SkipSentinel,
        "a single sentinel-emitting partition must invalidate the merged BF")
      // Reverse the merge order to pin the contract regardless of arrival order.
      val driverReverse = new BloomFilterBuildAccumulator()
      driverReverse.merge(poisoned)
      driverReverse.merge(real)
      assert(driverReverse.value eq BloomFilterBuildAccumulator.SkipSentinel,
        "merge order must not change the sentinel-wins outcome")
    }
  }

  test("sentinel survives executor-side serialization round-trip") {
    // BloomFilterBuildAccumulator's writeReplace branches on `atDriverSide`. The driver
    // branch serializes a zeroed copyAndReset(), so a naive driver-local round-trip would
    // never exercise the executor-to-driver path that ships the build result. Force the
    // executor-side branch by registering on the driver and doing an initial round-trip
    // that flips `atDriverSide` to false via AccumulatorV2.readObject's flip logic, then
    // mutate the deserialized copy and ship it back the way Spark would.
    val sc = spark.sparkContext
    val driverAcc = new BloomFilterBuildAccumulator()
    sc.register(driverAcc, "cubf-roundtrip-test")
    val executorAcc = javaRoundTrip(driverAcc)
    executorAcc.add(BloomFilterBuildAccumulator.SkipSentinel)
    val driverCopy = javaRoundTrip(executorAcc)
    val merged = new BloomFilterBuildAccumulator()
    merged.merge(driverCopy)
    // Use the public poison-wins contract to detect the sentinel without relying on the
    // private isSkipShape predicate: a subsequent add of any real BF must not unseat it.
    merged.add(makeBfBytes(version = 1, dataLastByte = 0x42))
    assert(merged.value eq BloomFilterBuildAccumulator.SkipSentinel,
      "an executor-emitted sentinel must survive Java serialization and merge")
  }

  private def javaRoundTrip[A <: AnyRef](obj: A): A = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    try oos.writeObject(obj) finally oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray))
    try ois.readObject().asInstanceOf[A] finally ois.close()
  }

  test("buildCostUpdaters do not break canonical transparency") {
    val child = stubChild()
    val metric1 = new CuBFDiagPairMetric
    val metric2 = new CuBFDiagPairMetric
    val specs = Seq(
      BFSpec("bf-A", 0, 5, 100000L),
      BFSpec("bf-B", 1, 5, 100000L))
    val with1 = GpuGenerateBloomFilterExec(
      specs = specs, bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = child,
      buildCostUpdaters = Map("bf-A" -> metric1, "bf-B" -> metric1))
    val with2 = GpuGenerateBloomFilterExec(
      specs = specs, bfVersion = 1, seed = 0, xxHashSeed = 42L,
      child = child,
      buildCostUpdaters = Map("bf-A" -> metric2, "bf-B" -> metric2))
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
