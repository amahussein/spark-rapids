/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.CheckOverflowInTableInsert
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.execution.{CollectLimitExec, GlobalLimitExec, SparkPlan}
import org.apache.spark.sql.rapids.GpuCheckOverflowInTableInsert

object SparkShimImpl extends Spark321PlusDBShims with Spark331PlusShims {
  override def getSparkShimVersion: ShimVersion = ShimLoader.getShimVersion

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
        TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (globalLimit, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimit, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(
              globalLimit.limit, childPlans.head.convertIfNeeded(), globalLimit.offset)
        }),
    GpuOverrides.exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
        TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (collectLimit, conf, p, r) => new GpuCollectLimitMeta(collectLimit, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuGlobalLimitExec(collectLimit.limit,
            GpuShuffleExchangeExec(
              GpuSinglePartitioning,
              GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded()),
              ENSURE_REQUIREMENTS
            )(SinglePartition), collectLimit.offset)
      }
    ).disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
      "of rows in a batch it could help by limiting the number of rows transferred from " +
      "GPU to CPU")
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      // Add expression CheckOverflowInTableInsert starting Spark-3.3.1+
      // Accepts all types as input as the child Cast does the type checking and the calculations.
      GpuOverrides.expr[CheckOverflowInTableInsert](
        "Casting a numeric value as another numeric type in store assignment",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.all,
          TypeSig.all),
        (t, conf, p, r) => new UnaryExprMeta[CheckOverflowInTableInsert](t, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = {
            child match {
              case c: GpuCast => GpuCheckOverflowInTableInsert(c, t.columnName)
              case _ =>
                throw new IllegalStateException("Expression child is not of Type GpuCast")
            }
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map
  }

}