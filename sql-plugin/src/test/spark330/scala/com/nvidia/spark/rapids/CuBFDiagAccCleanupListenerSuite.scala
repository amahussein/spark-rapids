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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

class CuBFDiagAccCleanupListenerSuite extends AnyFunSuite
    with CuBFLocalSparkSuite
    with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    clearSqlExecutionId()
    BloomFilterBuildCostAccumulator.clearAllForTests()
    BloomFilterProbeAccumulator.clearAllForTests()
    super.afterEach()
  }

  test("diagnostic accumulators are cached by SQL execution id and bfId") {
    val sc = spark.sparkContext
    var firstFor101: BloomFilterBuildCostAccumulator = null
    withSqlExecutionId(101L) {
      val first = BloomFilterBuildCostAccumulator.driverGetOrCreate(sc, "bf-A")
      val second = BloomFilterBuildCostAccumulator.driverGetOrCreate(sc, "bf-A")
      assert(first eq second)
      assert(first.name.contains("cubf_build_101_bf-A"))
      firstFor101 = first
    }
    withSqlExecutionId(102L) {
      val third = BloomFilterBuildCostAccumulator.driverGetOrCreate(sc, "bf-A")
      assert(third ne firstFor101)
      assert(third.name.contains("cubf_build_102_bf-A"))
      assert(BloomFilterBuildCostAccumulator.cacheSizeForTests === 2)
    }
  }

  test("no-SQL fallback registers fresh accumulators without caching") {
    val sc = spark.sparkContext
    val first = BloomFilterProbeAccumulator.driverGetOrCreate(sc, "bf-no-sql")
    val second = BloomFilterProbeAccumulator.driverGetOrCreate(sc, "bf-no-sql")
    assert(first ne second)
    assert(first.name.contains("cubf_probe_no_sql_bf-no-sql"))
    assert(second.name.contains("cubf_probe_no_sql_bf-no-sql"))
    assert(BloomFilterProbeAccumulator.cacheSizeForTests === 0)
  }

  test("cleanup listener removes only diagnostic caches for the completed execution") {
    val sc = spark.sparkContext
    withSqlExecutionId(201L) {
      BloomFilterBuildCostAccumulator.driverGetOrCreate(sc, "bf-1")
      BloomFilterProbeAccumulator.driverGetOrCreate(sc, "bf-1")
    }
    withSqlExecutionId(202L) {
      BloomFilterBuildCostAccumulator.driverGetOrCreate(sc, "bf-2")
      BloomFilterProbeAccumulator.driverGetOrCreate(sc, "bf-2")
    }

    new CuBFDiagAccCleanupListener()
      .onOtherEvent(SparkListenerSQLExecutionEnd(201L, System.currentTimeMillis()))

    assert(!BloomFilterBuildCostAccumulator.containsForTests(201L, "bf-1"))
    assert(!BloomFilterProbeAccumulator.containsForTests(201L, "bf-1"))
    assert(BloomFilterBuildCostAccumulator.containsForTests(202L, "bf-2"))
    assert(BloomFilterProbeAccumulator.containsForTests(202L, "bf-2"))
  }

}
