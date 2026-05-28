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

import com.nvidia.spark.rapids.cubf.CuBFDiagPairMetric
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{BinaryType, LongType}

/** Tests `GpuBloomFilterMightContain` metrics wiring and canonicalization. */
class GpuBloomFilterMightContainSuite extends AnyFunSuite {

  private def newExpr(
      bfId: Option[String],
      updater: Option[CuBFDiagPairMetric]): GpuBloomFilterMightContain = {
    GpuBloomFilterMightContain(
      bloomFilterExpression = Literal(null, BinaryType),
      valueExpression = Literal(0L, LongType),
      bfId = bfId,
      probeUpdater = updater)
  }

  test("recordBatchUpdate records one pair per call") {
    val metric = new CuBFDiagPairMetric
    val expr = newExpr(bfId = Some("cubf-test-r7"), updater = Some(metric))
    expr.recordBatchUpdate(1000000L, 700000L)
    assert(metric.value === ((1000000L, 700000L)),
      "metric must record one predicate batch update, never per row")
  }

  test("multi-batch updates sum the pair components") {
    val metric = new CuBFDiagPairMetric
    val expr = newExpr(bfId = Some("cubf-test-multi"), updater = Some(metric))
    expr.recordBatchUpdate(500000L, 350000L)
    expr.recordBatchUpdate(500000L, 200000L)
    expr.recordBatchUpdate(500000L, 150000L)
    assert(metric.value === ((1500000L, 700000L)),
      "3 batches must produce the summed rows-in and rows-passed pair")
  }

  test("recordBatchUpdate is a no-op when probeUpdater is None") {
    // A metric wired to a sibling expression must not see invocations from the
    // None-updater expression. Catches .foreach -> .get refactors and any
    // future cross-instance side-effect leak.
    val metric = new CuBFDiagPairMetric
    val sibling = newExpr(bfId = Some("cubf-active"), updater = Some(metric))
    val target = newExpr(bfId = Some("cubf-no-updater"), updater = None)
    target.recordBatchUpdate(1000000L, 700000L)
    assert(metric.value === ((0L, 0L)),
      "None-updater path must not fire any other expression's metric")
    // Sanity check: the metric fires for its own owner.
    sibling.recordBatchUpdate(1L, 1L)
    assert(metric.value === ((1L, 1L)))
  }

  test("canonicalized drops bfId so distinct instances compare equal") {
    val metric1 = new CuBFDiagPairMetric
    val metric2 = new CuBFDiagPairMetric
    val a = newExpr(bfId = Some("cubf-aaaa"), updater = Some(metric1))
    val b = newExpr(bfId = Some("cubf-bbbb"), updater = Some(metric2))
    val c = newExpr(bfId = None, updater = None)
    assert(a.canonicalized == b.canonicalized,
      "bfId difference must be erased by canonicalize")
    assert(a.canonicalized == c.canonicalized,
      "presence vs absence of bfId/probeUpdater must canonicalize equal")
  }

  test("canonicalized leaves bfId / probeUpdater as None") {
    val metric = new CuBFDiagPairMetric
    val a = newExpr(bfId = Some("cubf-xyz"), updater = Some(metric))
    val canon = a.canonicalized.asInstanceOf[GpuBloomFilterMightContain]
    assert(canon.bfId.isEmpty)
    assert(canon.probeUpdater.isEmpty)
  }
}
