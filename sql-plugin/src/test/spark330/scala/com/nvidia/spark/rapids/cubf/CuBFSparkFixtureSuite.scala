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

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SQLExecution

trait CuBFSparkFixtureSuite extends BeforeAndAfterAll { this: Suite =>

  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
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

  protected def clearSqlExecutionId(): Unit = {
    spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
  }

  protected def withSqlExecutionId(id: Long)(body: => Unit): Unit = {
    val sc = spark.sparkContext
    sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, id.toString)
    try {
      body
    } finally {
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
    }
  }

  protected def withSqlConf(pairs: (String, String)*)(body: => Unit): Unit = {
    val previous = pairs.map { case (key, _) => key -> spark.conf.getOption(key) }
    pairs.foreach { case (key, value) => spark.conf.set(key, value) }
    try {
      body
    } finally {
      previous.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  protected def withClearedDiagMetricCaches[T](body: => T): T = {
    CuBFDiagPairMetric.clearAllForTests()
    try {
      body
    } finally {
      CuBFDiagPairMetric.clearAllForTests()
    }
  }
}
