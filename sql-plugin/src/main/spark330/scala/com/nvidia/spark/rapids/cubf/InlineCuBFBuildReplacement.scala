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

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Replaces the optional planner's InlineCuBFBuildExec with `GpuGenerateCuBFExec`.
 *
 * Reflection keeps this rule inert when the planner module is absent.
 */
case class InlineCuBFBuildReplacement() extends Rule[SparkPlan] with Logging {

  import InlineCuBFBuildReplacement._

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case exec if exec.getClass.getName == inlineCuBFClassName =>
        replaceWithGpu(exec)
    }
  }

  private def replaceWithGpu(exec: SparkPlan): SparkPlan = {
    try {
      val bfVersion = getField[Int](exec, "bfVersion")
      val seed = getField[Int](exec, "seed")
      val xxHashSeed = getField[Long](exec, "xxHashSeed")
      val child = getField[SparkPlan](exec, "child")
      val specs = readSpecs(exec)
      val bfIdsCsv = specs.map(_.bfId).mkString(",")
      val keyIdxCsv = specs.map(_.keyColumnIndex).mkString(",")
      val numHashesCsv = specs.map(_.numHashes).mkString(",")
      val numBitsCsv = specs.map(_.numBits).mkString(",")
      logInfo(s"[CuBF-GpuOverride] Replacing InlineCuBFBuildExec " +
        s"with GpuGenerateCuBFExec bfIds=[$bfIdsCsv] " +
        s"keyIdxes=[$keyIdxCsv] numHashes=[$numHashesCsv] " +
        s"numBits=[$numBitsCsv] version=$bfVersion")
      val updaters = resolveBuildCostUpdaters(specs.map(_.bfId))
      GpuGenerateCuBFExec(specs, bfVersion, seed,
        xxHashSeed, child, updaters)
    } catch {
      case NonFatal(e) =>
        logWarning(s"[CuBF-GpuOverride] Failed to replace " +
          s"InlineCuBFBuildExec: ${e.getMessage}. " +
          s"Keeping CPU stub (BF will not be built).")
        exec
    }
  }

  /** Creates diagnostic build-cost updaters when cuBF markers carry bfIds. */
  private[rapids] def resolveBuildCostUpdaters(
      bfIds: Seq[String]): Map[String, CuBFDiagPairMetric] = {
    val usableBfIds = bfIds.filter(CuBFDiagPairMetric.isUsableBfId)
    if (usableBfIds.isEmpty ||
        !RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.get(SQLConf.get)) {
      return Map.empty
    }
    SparkSession.getActiveSession match {
      case Some(spark) =>
        usableBfIds.map { bfId =>
          val acc = CuBFDiagPairMetric.buildForBfId(spark.sparkContext, bfId)
          bfId -> acc
        }.toMap
      case None => Map.empty
    }
  }

  /** Reads current multi-spec shape, falling back to the legacy single-spec shape. */
  private[rapids] def readSpecs(exec: Any): Seq[CuBFSpec] = {
    val execClass = exec.getClass
    val specsMethod = try Some(execClass.getMethod("specs")) catch {
      case _: NoSuchMethodException => None
    }
    specsMethod match {
      case Some(m) =>
        val rawSpecs = m.invoke(exec).asInstanceOf[Seq[_]]
        rawSpecs.map { specObj =>
          CuBFSpec(
            bfId = getField[String](specObj, "bfId"),
            keyColumnIndex = getField[Int](specObj, "keyColumnIndex"),
            numHashes = getField[Int](specObj, "numHashes"),
            numBits = getField[Long](specObj, "numBits"))
        }.toSeq
      case None =>
        // Legacy single-spec InlineCuBFBuildExec shape.
        val legacySpec = CuBFSpec(
          bfId = getField[String](exec, "bfId"),
          keyColumnIndex = getField[Int](exec, "keyColumnIndex"),
          numHashes = getField[Int](exec, "numHashes"),
          numBits = getField[Long](exec, "numBits"))
        Seq(legacySpec)
    }
  }

  private def getField[T](obj: Any, name: String): T = {
    val method = obj.getClass.getMethod(name)
    method.invoke(obj).asInstanceOf[T]
  }
}

object InlineCuBFBuildReplacement {
  // Fully qualified class name of the optional planner's CPU stub.
  private val inlineCuBFClassName =
    "com.nvidia.spark.rapids.optimizer.cubf.InlineCuBFBuildExec"

  def applyIfNeeded(plan: SparkPlan): SparkPlan = {
    if (isNeeded(plan)) {
      InlineCuBFBuildReplacement().apply(plan)
    } else {
      plan
    }
  }

  // Avoid transformUp unless the optional inline-build node is present.
  def isNeeded(plan: SparkPlan): Boolean = {
    plan.find(_.getClass.getName == inlineCuBFClassName).isDefined
  }
}
