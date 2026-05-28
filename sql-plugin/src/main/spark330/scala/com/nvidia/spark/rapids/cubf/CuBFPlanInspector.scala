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

import scala.collection.mutable
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

// Diagnostic-metrics helpers used by BloomFilterShims to extract the optional bfId
// from a probe expression's subquery plan.
private[cubf] object CuBFPlanInspector {

  // Optional planner leaf that carries the bfId for metrics wiring.
  private val TryReadBFRegistryExecClassName =
    "com.nvidia.spark.rapids.optimizer.cubloomfilter.TryReadBFRegistryExec"

  private val AqePlanFields: Seq[String] =
    Seq("executedPlan", "currentPhysicalPlan", "initialPlan", "inputPlan")

  private[cubf] def extractBfId(expr: Expression): Option[String] = {
    var found: Option[String] = None
    expr.foreach {
      case e: ExecSubqueryExpression if found.isEmpty =>
        found = findBfIdInPlan(e.plan)
      case _ =>
    }
    found
  }

  /**
   * Contract: `CuBloomFilterPostDPPRule` applies global candidate ranking and the per-query BF
   * quota before injection. Each selected probe predicate gets its own `BloomFilterMightContain`
   * scalar subquery with at most one `TryReadBFRegistryExec`. Multi-key fact-side stacking may put
   * multiple probe predicates on one child, and older single-BF shapes may put just one, but a
   * single scalar subquery must never contain multiple registry reads. If a future planner shape
   * violates that invariant, diagnostic attribution is ambiguous.
   */
  private[cubf] def findBfIdInPlan(plan: SparkPlan): Option[String] = {
    val found = findAllBfIdsInPlan(plan)
    assert(found.size <= 1,
      s"Expected at most one cuBF registry read per BloomFilterMightContain subquery, " +
        s"found ${found.size}: ${found.mkString(", ")}")
    found.headOption
  }

  private def findAllBfIdsInPlan(plan: SparkPlan): Seq[String] = {
    val found = mutable.ArrayBuffer.empty[String]
    val visited = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[SparkPlan, java.lang.Boolean]())
    def visit(p: SparkPlan): Unit = {
      if (p != null && visited.add(p)) {
        if (p.getClass.getName == TryReadBFRegistryExecClassName) {
          readBfId(p).foreach(found += _)
        }
        p match {
          case s: BaseSubqueryExec => visit(s.child)
          case _ =>
        }
        tryAqePlanFields(p).foreach(visit)
        p.children.foreach(visit)
      }
    }
    visit(plan)
    found.toSeq
  }

  private def readBfId(plan: SparkPlan): Option[String] = {
    try {
      Option(plan.getClass.getMethod("bfId").invoke(plan).asInstanceOf[String])
        .filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }
  }

  private[cubf] def tryAqePlanFields(p: SparkPlan): Seq[SparkPlan] = {
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

private[rapids] object CuBFProbeDiagWiring {

  def resolveProbeWiring(
      bloomFilterExpression: Expression
  ): (Option[String], Option[CuBFDiagPairMetric]) = {
    if (!RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.get(SQLConf.get)) {
      (None, None)
    } else {
      // Diagnostic-only wiring: carry the planner bfId to GpuBloomFilterMightContain and
      // attach a probe accumulator for Spark UI/event-log observability.
      val bfIdOpt = CuBFPlanInspector.extractBfId(bloomFilterExpression)
        .filter(CuBFDiagPairMetric.isUsableBfId)
      val updaterOpt = for {
        bfId <- bfIdOpt
        spark <- SparkSession.getActiveSession
      } yield CuBFDiagPairMetric.probeForBfId(spark.sparkContext, bfId)
      (bfIdOpt, updaterOpt)
    }
  }
}
