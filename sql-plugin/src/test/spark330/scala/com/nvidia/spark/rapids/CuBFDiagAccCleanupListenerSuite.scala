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

import com.nvidia.spark.rapids.cubf.{CuBFDiagAccCleanupListener, CuBFDiagPairMetric,
  CuBFSparkFixtureSuite}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

class CuBFDiagAccCleanupListenerSuite extends AnyFunSuite
    with CuBFSparkFixtureSuite
    with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    clearSqlExecutionId()
    CuBFDiagPairMetric.clearAllForTests()
    super.afterEach()
  }

  test("diagnostic accumulators are cached by SQL execution id and bfId") {
    val sc = spark.sparkContext
    var firstFor101: CuBFDiagPairMetric = null
    withSqlExecutionId(101L) {
      val first = CuBFDiagPairMetric.buildForBfId(sc, "bf-A")
      val second = CuBFDiagPairMetric.buildForBfId(sc, "bf-A")
      assert(first eq second)
      assert(first.name.contains("cubf_build_101_bf-A"))
      firstFor101 = first
    }
    withSqlExecutionId(102L) {
      val third = CuBFDiagPairMetric.buildForBfId(sc, "bf-A")
      assert(third ne firstFor101)
      assert(third.name.contains("cubf_build_102_bf-A"))
      assert(CuBFDiagPairMetric.buildCacheSize === 2)
    }
  }

  test("no-SQL fallback registers fresh accumulators without caching") {
    val sc = spark.sparkContext
    val first = CuBFDiagPairMetric.probeForBfId(sc, "bf-no-sql")
    val second = CuBFDiagPairMetric.probeForBfId(sc, "bf-no-sql")
    assert(first ne second)
    assert(first.name.contains("cubf_probe_no_sql_bf-no-sql"))
    assert(second.name.contains("cubf_probe_no_sql_bf-no-sql"))
    assert(CuBFDiagPairMetric.probeCacheSize === 0)
  }

  test("cleanup listener removes only diagnostic caches for the completed execution") {
    val sc = spark.sparkContext
    withSqlExecutionId(201L) {
      CuBFDiagPairMetric.buildForBfId(sc, "bf-1")
      CuBFDiagPairMetric.probeForBfId(sc, "bf-1")
    }
    withSqlExecutionId(202L) {
      CuBFDiagPairMetric.buildForBfId(sc, "bf-2")
      CuBFDiagPairMetric.probeForBfId(sc, "bf-2")
    }

    new CuBFDiagAccCleanupListener()
      .onOtherEvent(SparkListenerSQLExecutionEnd(201L, System.currentTimeMillis()))

    assert(!CuBFDiagPairMetric.buildContains(201L, "bf-1"))
    assert(!CuBFDiagPairMetric.probeContains(201L, "bf-1"))
    assert(CuBFDiagPairMetric.buildContains(202L, "bf-2"))
    assert(CuBFDiagPairMetric.probeContains(202L, "bf-2"))
  }
}
