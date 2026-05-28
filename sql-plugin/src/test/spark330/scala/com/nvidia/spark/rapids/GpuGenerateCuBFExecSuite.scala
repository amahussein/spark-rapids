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

import com.nvidia.spark.rapids.CuBFTestHelpers._
import com.nvidia.spark.rapids.cubf.{CuBFBuildResultAccumulator, CuBFDiagPairMetric}
import com.nvidia.spark.rapids.cubf.{CuBFSpec, GpuGenerateCuBFExec, InlineCuBFBuildReplacement}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.SparkPlan

/** Tests multi-spec `GpuGenerateCuBFExec` driver-side behavior. */
class GpuGenerateCuBFExecSuite extends AnyFunSuite
    with CuBFLocalSparkSuite {

  private def stubChild(): SparkPlan = spark.range(0).queryExecution.executedPlan

  private def newExec(
      specs: Seq[CuBFSpec],
      buildCostUpdaters: Map[String, CuBFDiagPairMetric] = Map.empty,
      bfVersion: Int = 1,
      child: SparkPlan = stubChild()): GpuGenerateCuBFExec =
    GpuGenerateCuBFExec(
      specs = specs,
      bfVersion = bfVersion,
      seed = 0,
      xxHashSeed = 42L,
      child = child,
      buildCostUpdaters = buildCostUpdaters)

  private def bfSpec(
      id: String,
      keyColumnIndex: Int = 0,
      numHashes: Int = 5,
      bits: Long = 100000L): CuBFSpec =
    CuBFSpec(id, keyColumnIndex = keyColumnIndex, numHashes = numHashes, numBits = bits)

  Seq(
    "multi-spec registers 3 accumulators" ->
      Seq(
        bfSpec("bf-A", keyColumnIndex = 0),
        bfSpec("bf-B", keyColumnIndex = 1),
        bfSpec("bf-C", keyColumnIndex = 2)),
    "single-spec registers one accumulator" ->
      Seq(bfSpec("single", numHashes = 7, bits = 524288L))
  ).foreach { case (testName, specs) =>
    test(testName) {
      val exec = newExec(specs)
      val accs = exec.accumulators
      assert(accs.size == specs.size,
        s"expected ${specs.size} accumulators, got ${accs.size}")
      assert(accs.keySet == specs.map(_.bfId).toSet,
        s"unexpected bfId set: ${accs.keySet}")
      specs.foreach { spec =>
        val acc = accs(spec.bfId)
        assert(acc.name.contains(s"cuBF-${spec.bfId}"),
          s"accumulator for ${spec.bfId} has name ${acc.name}, " +
            s"expected Some(cuBF-${spec.bfId})")
      }
    }
  }

  Seq(1, 2).foreach { version =>
    test(s"multi-spec produces N BFs with no cross-contamination [v$version]") {
      val specs = Seq(
        bfSpec("bf-A", keyColumnIndex = 0, numHashes = 1, bits = 64),
        bfSpec("bf-B", keyColumnIndex = 1, numHashes = 1, bits = 64))
      val exec = newExec(specs, bfVersion = version)
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
    val specs = Seq(
      bfSpec("bf-A", keyColumnIndex = 0),
      bfSpec("bf-B", keyColumnIndex = 1))
    val execSingle = newExec(Seq(bfSpec("single")), child = child)
    val execMulti = newExec(specs, child = child)
    assert(execSingle.canonicalized == child.canonicalized,
      "single-spec GpuGenerateCuBFExec must be transparent")
    assert(execMulti.canonicalized == child.canonicalized,
      "multi-spec GpuGenerateCuBFExec must be transparent")
    val execMulti2 = newExec(specs, child = child)
    assert(execMulti.canonicalized == execMulti2.canonicalized,
      "sibling multi-spec wrappers with identical specs must " +
        "canonicalize equally (ReuseExchange precondition)")
  }

  test("accumulator markSkipped publishes sentinel value") {
    val acc = new CuBFBuildResultAccumulator()
    assert(acc.isZero)
    acc.markSkipped()
    assert(!acc.isZero)
    assert(acc.value eq CuBFBuildResultAccumulator.SkipSentinel,
      "markSkipped must leave the sentinel identity in `value`")
  }

  private def testSentinelMerge(
      label: String,
      versions: Seq[Int] = Seq(1, 2))(
      append: (CuBFBuildResultAccumulator, Int) => Unit): Unit = {
    versions.foreach { version =>
      val suffix = if (versions.size > 1) s" [v$version]" else ""
      test(s"accumulator merge $label$suffix") {
        val acc = new CuBFBuildResultAccumulator()
        append(acc, version)
        assert(acc.value eq CuBFBuildResultAccumulator.SkipSentinel,
          "merge must canonicalize to the sentinel identity")
      }
    }
  }

  testSentinelMerge("sentinel with serialized sentinel yields sentinel", Seq(1)) {
    (acc, _) =>
      acc.markSkipped()
      acc.add(Array[Byte](0, 0, 0, 0))
  }

  testSentinelMerge("sentinel then real BF yields sentinel") { (acc, version) =>
    acc.add(CuBFBuildResultAccumulator.SkipSentinel.clone())
    acc.add(makeBfBytes(version = version, dataLastByte = 0x42))
  }

  testSentinelMerge("real BF then sentinel yields sentinel") { (acc, version) =>
    acc.add(makeBfBytes(version = version, dataLastByte = 0x42))
    acc.add(CuBFBuildResultAccumulator.SkipSentinel.clone())
  }

  Seq(1, 2).foreach { version =>
    test(s"accumulator merge real with real does not canonicalize to sentinel [v$version]") {
      val acc = new CuBFBuildResultAccumulator()
      acc.add(makeBfBytes(version = version, dataLastByte = 0x0F))
      acc.add(makeBfBytes(version = version, dataLastByte = 0xF0))
      assert(acc.value ne CuBFBuildResultAccumulator.SkipSentinel,
        "two real BFs must not canonicalize to the sentinel")
      assert((acc.value(headerSize(version) + 7) & 0xFF) == 0xFF,
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
      val acc = new CuBFBuildResultAccumulator()
      acc.add(bytes)
      java.util.Arrays.fill(bytes, 0.toByte)
      assert(java.util.Arrays.equals(acc.value, expected),
        "accumulator value must not alias the caller-provided bytes")
      assert(acc.value ne CuBFBuildResultAccumulator.SkipSentinel,
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
    withClearedDiagMetricCaches {
      withSqlConf(RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.key -> "false") {
        withSqlExecutionId(301L) {
          val updaters = InlineCuBFBuildReplacement()
            .resolveBuildCostUpdaters(Seq("bf-diag-off"))
          assert(updaters.isEmpty)
          assert(CuBFDiagPairMetric.buildCacheSize === 0)
        }
      }

      withSqlConf(RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.key -> "true") {
        withSqlExecutionId(302L) {
          val updaters = InlineCuBFBuildReplacement()
            .resolveBuildCostUpdaters(Seq("", "cubf-", "bf-diag-on"))
          assert(updaters.keySet === Set("bf-diag-on"))
          assert(CuBFDiagPairMetric.buildContains(302L, "bf-diag-on"))
          assert(!CuBFDiagPairMetric.buildContains(302L, ""))
          assert(!CuBFDiagPairMetric.buildContains(302L, "cubf-"))
        }
        withSqlExecutionId(303L) {
          val updaters = InlineCuBFBuildReplacement().resolveBuildCostUpdaters(Seq.empty)
          assert(updaters.isEmpty)
          assert(!CuBFDiagPairMetric.buildContains(303L, ""))
        }
      }
    }
  }

  test("resolveEffectiveMaxFilterBytes is fail-safe on missing capability helper") {
    val cap = GpuGenerateCuBFExec.resolveEffectiveMaxFilterBytes()
    val v1Ceiling = (1L << 31) / 8L
    assert(cap == v1Ceiling,
      s"expected V1 ceiling ($v1Ceiling); got $cap. " +
        "the helper must fail closed to the V1 cap.")
  }

  test("requires nonEmpty specs") {
    val ex = intercept[IllegalArgumentException] {
      newExec(Seq.empty)
    }
    assert(ex.getMessage.contains("at least one"),
      s"unexpected message: ${ex.getMessage}")
  }

  test("recordBuildUpdate records one pair per BF build") {
    val metric = new CuBFDiagPairMetric
    val exec = newExec(
      Seq(bfSpec("cubf-r7-single")),
      buildCostUpdaters = Map("cubf-r7-single" -> metric))
    exec.recordBuildUpdate("cubf-r7-single", 12345678L, 4096L)
    assert(metric.value === ((12345678L, 4096L)),
      "metric must record one BF build update, never per batch or per row")
  }

  test("multi-BF build records each metric independently") {
    val metricA = new CuBFDiagPairMetric
    val metricB = new CuBFDiagPairMetric
    val metricC = new CuBFDiagPairMetric
    val exec = newExec(
      Seq(
        bfSpec("cubf-r7-A", keyColumnIndex = 0),
        bfSpec("cubf-r7-B", keyColumnIndex = 1, bits = 200000L),
        bfSpec("cubf-r7-C", keyColumnIndex = 2, bits = 300000L)),
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
    val sibling = newExec(
      Seq(bfSpec("cubf-active")),
      buildCostUpdaters = Map("cubf-active" -> metric))
    val target = newExec(Seq(bfSpec("cubf-no-updater")))
    target.recordBuildUpdate("cubf-no-updater", 1000000L, 8192L)
    assert(metric.value === ((0L, 0L)),
      "empty buildCostUpdaters path must not fire any other exec's metric")
    // Sanity check: the metric fires for its own owner.
    sibling.recordBuildUpdate("cubf-active", 1L, 1L)
    assert(metric.value === ((1L, 1L)))
  }

  test("recordBuildUpdate is a no-op when bfId is not in the map") {
    val metric = new CuBFDiagPairMetric
    val exec = newExec(
      Seq(bfSpec("cubf-known")),
      buildCostUpdaters = Map("cubf-known" -> metric))
    exec.recordBuildUpdate("cubf-unknown", 1000L, 512L)
    assert(metric.value === ((0L, 0L)),
      "metric must not update for an unknown bfId")
  }

  test("isNeeded returns false for plan without markers") {
    val plan = spark.range(10).queryExecution.executedPlan
    assert(!InlineCuBFBuildReplacement.isNeeded(plan),
      "isNeeded must return false when no InlineBFBuildExec markers are present")
  }

  test("applyIfNeeded returns plan unchanged when no markers are present") {
    val plan = spark.range(10).queryExecution.executedPlan
    assert(InlineCuBFBuildReplacement.applyIfNeeded(plan) eq plan,
      "applyIfNeeded must return the original plan reference unchanged")
  }

  Seq(1, 2).foreach { version =>
    test(s"driver-side merge of one real and one sentinel partition yields sentinel [v$version]") {
      // Each partition ships its own accumulator copy back to the driver. If one partition
      // emits the skip sentinel because it closed early, the driver-side merge must promote
      // the merged value to the sentinel so a probe filter built from a strict subset of
      // build keys cannot silently drop matching probe rows.
      val real = new CuBFBuildResultAccumulator()
      real.add(makeBfBytes(version = version, dataLastByte = 0x42))
      val poisoned = new CuBFBuildResultAccumulator()
      poisoned.add(CuBFBuildResultAccumulator.SkipSentinel)
      val driver = new CuBFBuildResultAccumulator()
      driver.merge(real)
      driver.merge(poisoned)
      assert(driver.value eq CuBFBuildResultAccumulator.SkipSentinel,
        "a single sentinel-emitting partition must invalidate the merged BF")
      // Reverse the merge order to pin the contract regardless of arrival order.
      val driverReverse = new CuBFBuildResultAccumulator()
      driverReverse.merge(poisoned)
      driverReverse.merge(real)
      assert(driverReverse.value eq CuBFBuildResultAccumulator.SkipSentinel,
        "merge order must not change the sentinel-wins outcome")
    }
  }

  test("sentinel survives executor-side serialization round-trip") {
    // CuBFBuildResultAccumulator's writeReplace branches on `atDriverSide`. The driver
    // branch serializes a zeroed copyAndReset(), so a naive driver-local round-trip would
    // never exercise the executor-to-driver path that ships the build result. Force the
    // executor-side branch by registering on the driver and doing an initial round-trip
    // that flips `atDriverSide` to false via AccumulatorV2.readObject's flip logic, then
    // mutate the deserialized copy and ship it back the way Spark would.
    val sc = spark.sparkContext
    val driverAcc = new CuBFBuildResultAccumulator()
    sc.register(driverAcc, "cubf-roundtrip-test")
    val executorAcc = javaRoundTrip(driverAcc)
    executorAcc.add(CuBFBuildResultAccumulator.SkipSentinel)
    val driverCopy = javaRoundTrip(executorAcc)
    val merged = new CuBFBuildResultAccumulator()
    merged.merge(driverCopy)
    // Use the public poison-wins contract to detect the sentinel without relying on the
    // private isSkipShape predicate: a subsequent add of any real BF must not unseat it.
    merged.add(makeBfBytes(version = 1, dataLastByte = 0x42))
    assert(merged.value eq CuBFBuildResultAccumulator.SkipSentinel,
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
      bfSpec("bf-A", keyColumnIndex = 0),
      bfSpec("bf-B", keyColumnIndex = 1))
    val with1 = newExec(
      specs,
      buildCostUpdaters = Map("bf-A" -> metric1, "bf-B" -> metric1),
      child = child)
    val with2 = newExec(
      specs,
      buildCostUpdaters = Map("bf-A" -> metric2, "bf-B" -> metric2),
      child = child)
    val without = newExec(specs, child = child)
    assert(with1.canonicalized == with2.canonicalized,
      "different buildCostUpdaters must canonicalize equal " +
        "for exchange reuse")
    assert(with1.canonicalized == without.canonicalized,
      "presence vs absence of buildCostUpdaters must canonicalize equal")
    assert(with1.canonicalized == child.canonicalized,
      "canonical form drops the wrapper entirely")
  }
}
