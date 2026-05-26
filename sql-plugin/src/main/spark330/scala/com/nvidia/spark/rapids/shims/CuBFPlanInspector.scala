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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.{BloomFilterLongPairAccumulator, BloomFilterPredicateUpdater,
  BloomFilterProbeAccumulator, RapidsConf}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

// Plan-walking helpers used by BloomFilterShims to extract the optional bfId
// from a probe expression's subquery plan.
object CuBFPlanInspector {

  // Optional planner leaf that carries the bfId for metrics wiring.
  private val TryReadBFRegistryExecClassName =
    "com.nvidia.spark.rapids.optimizer.cubloomfilter.TryReadBFRegistryExec"

  private val AqePlanFields: Seq[String] =
    Seq("executedPlan", "currentPhysicalPlan", "initialPlan", "inputPlan")

  private[shims] def extractBfId(expr: Expression): Option[String] = {
    var found: Option[String] = None
    expr.foreach {
      case e: ExecSubqueryExpression if found.isEmpty =>
        found = findBfIdInPlan(e.plan)
      case _ =>
    }
    found
  }

  private[shims] def findBfIdInPlan(plan: SparkPlan): Option[String] = {
    // Each BloomFilterMightContain scalar subquery has at most one planner BF registry read.
    var found: Option[String] = None
    def visit(p: SparkPlan): Unit = {
      if (found.isEmpty && p != null) {
        if (p.getClass.getName == TryReadBFRegistryExecClassName) {
          found = readBfId(p)
        }
        if (found.isEmpty) {
          p match {
            case s: BaseSubqueryExec => visit(s.child)
            case _ =>
          }
        }
        if (found.isEmpty) {
          tryAqePlanFields(p).foreach(visit)
        }
        if (found.isEmpty) {
          p.children.foreach(visit)
        }
      }
    }
    visit(plan)
    found
  }

  private def readBfId(plan: SparkPlan): Option[String] = {
    try {
      Option(plan.getClass.getMethod("bfId").invoke(plan).asInstanceOf[String])
        .filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }
  }

  private[shims] def tryAqePlanFields(p: SparkPlan): Seq[SparkPlan] = {
    if (!p.getClass.getName.contains("AdaptiveSparkPlanExec")) {
      Seq.empty
    } else {
      AqePlanFields.flatMap { name =>
        try {
          val m = p.getClass.getMethod(name)
          Option(m.invoke(p).asInstanceOf[SparkPlan])
        } catch {
          case NonFatal(_) => None
        }
      }
    }
  }
}

private[shims] object CuBFProbeDiagWiring {

  def resolveProbeWiring(
      bloomFilterExpression: Expression
  ): (Option[String], Option[BloomFilterPredicateUpdater]) = {
    if (!RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.get(SQLConf.get)) {
      (None, None)
    } else {
      val bfIdOpt = CuBFPlanInspector.extractBfId(bloomFilterExpression)
        .filter(BloomFilterLongPairAccumulator.isUsableBfId)
      val updaterOpt = for {
        bfId <- bfIdOpt
        spark <- SparkSession.getActiveSession
      } yield BloomFilterProbeAccumulator.driverGetOrCreate(spark.sparkContext, bfId)
      (bfIdOpt, updaterOpt)
    }
  }
}
