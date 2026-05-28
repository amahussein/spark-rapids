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

// Test-scope stub for the optional planner class discovered by fully qualified class name.
package com.nvidia.spark.rapids.optimizer.cubloomfilter {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.Attribute
  import org.apache.spark.sql.execution.LeafExecNode

  /** Test-only stub for the optional planner class discovered by fully qualified class name. */
  case class TryReadBFRegistryExec(bfId: String) extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("test stub")
  }
}

package com.nvidia.spark.rapids.cubf {

  import org.scalatest.funsuite.AnyFunSuite

  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, NamedExpression}
  import org.apache.spark.sql.execution.{BinaryExecNode, LeafExecNode, SparkPlan, SubqueryExec,
    UnaryExecNode}

  import com.nvidia.spark.rapids.{CuBFLocalSparkSuite, RapidsConf}
  import com.nvidia.spark.rapids.optimizer.cubloomfilter.TryReadBFRegistryExec

  /** Regression coverage for AQE-aware `findBfIdInPlan`. */
  class CuBFPlanInspectorSuite extends AnyFunSuite
      with CuBFLocalSparkSuite {

    private case class FakeAdaptiveSparkPlanExec(inner: SparkPlan)
        extends LeafExecNode {
      override def output: Seq[Attribute] = Seq.empty
      def executedPlan: SparkPlan = inner
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
    }

    private case class FakeAdaptiveSparkPlanExecAlt(inner: SparkPlan)
        extends LeafExecNode {
      override def output: Seq[Attribute] = Seq.empty
      def currentPhysicalPlan: SparkPlan = inner
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
    }

    private case class PlainLeafExec() extends LeafExecNode {
      override def output: Seq[Attribute] = Seq.empty
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
    }

    private case class WrapExec(child: SparkPlan) extends UnaryExecNode {
      override def output: Seq[Attribute] = Seq.empty
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)
    }

    // Models the invalid single-subquery shape with two registry reads.
    private case class PairExec(left: SparkPlan, right: SparkPlan) extends BinaryExecNode {
      override def output: Seq[Attribute] = Seq.empty
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
      override protected def withNewChildrenInternal(
          newLeft: SparkPlan,
          newRight: SparkPlan): SparkPlan =
        copy(left = newLeft, right = newRight)
    }

    test("tryAqePlanFields: returns Seq.empty for non-AQE plans") {
      val plain = PlainLeafExec()
      assert(CuBFPlanInspector.tryAqePlanFields(plain).isEmpty,
        "non-AQE plan must not trigger reflective field probing")
    }

    test("tryAqePlanFields: returns inner plan for AQE-named class with executedPlan") {
      val inner = PlainLeafExec()
      val aqe = FakeAdaptiveSparkPlanExec(inner)
      val extracted = CuBFPlanInspector.tryAqePlanFields(aqe)
      assert(extracted.nonEmpty,
        "AQE-named plan with executedPlan must surface its inner plan")
      assert(extracted.head eq inner,
        "extracted reference must be the same instance the stub returned")
    }

    test("tryAqePlanFields: probes all 4 accessor names in priority order") {
      val inner = PlainLeafExec()
      val aqe = FakeAdaptiveSparkPlanExecAlt(inner)
      val extracted = CuBFPlanInspector.tryAqePlanFields(aqe)
      assert(extracted.nonEmpty,
        "fallback to currentPhysicalPlan must work when executedPlan is absent")
      assert(extracted.contains(inner),
        "currentPhysicalPlan's return value must be in the extracted list")
    }

    test("findBfIdInPlan: AQE-wrapped subquery regression case") {
      val tryRead = TryReadBFRegistryExec(bfId = "cubf-aqe-regression-test")
      val aqeWrapped = FakeAdaptiveSparkPlanExec(tryRead)
      val result = CuBFPlanInspector.findBfIdInPlan(aqeWrapped)
      assert(result === Some("cubf-aqe-regression-test"),
        "bfId must be discoverable through AQE wrapping")
    }

    test("findBfIdInPlan: non-AQE direct child path still works") {
      val tryRead = TryReadBFRegistryExec(bfId = "cubf-non-aqe-path")
      val wrapped = WrapExec(tryRead)
      val result = CuBFPlanInspector.findBfIdInPlan(wrapped)
      assert(result === Some("cubf-non-aqe-path"),
        "non-AQE path must remain working; the AQE helper must not " +
          "break the standard children traversal")
    }

    test("findBfIdInPlan: leaf TryReadBFRegistryExec at root") {
      val tryRead = TryReadBFRegistryExec(bfId = "cubf-root")
      assert(CuBFPlanInspector.findBfIdInPlan(tryRead) === Some("cubf-root"))
    }

    test("findBfIdInPlan: returns None when no TryReadBFRegistryExec is reachable") {
      val plain = PlainLeafExec()
      val aqeWrapped = FakeAdaptiveSparkPlanExec(plain)
      assert(CuBFPlanInspector.findBfIdInPlan(aqeWrapped).isEmpty,
        "absence of TryReadBFRegistryExec must yield None, not throw")
    }

    test("findBfIdInPlan: asserts on multiple registry reads in one subquery") {
      val left = TryReadBFRegistryExec(bfId = "cubf-left")
      val right = TryReadBFRegistryExec(bfId = "cubf-right")
      val err = intercept[AssertionError] {
        CuBFPlanInspector.findBfIdInPlan(PairExec(left, right))
      }
      assert(err.getMessage.contains("at most one cuBF registry read"),
        s"unexpected assertion message: ${err.getMessage}")
    }

    test("probe diagnostic wiring requires a private marker and usable bfId") {
      CuBFDiagPairMetric.clearAllForTests()
      try {
        withSqlConf(RapidsConf.CUBF_DIAGNOSTIC_METRICS_ENABLED.key -> "true") {
          withSqlExecutionId(401L) {
            val (noMarkerBfId, noMarkerUpdater) =
              CuBFProbeDiagWiring.resolveProbeWiring(Literal(1))
            assert(noMarkerBfId.isEmpty)
            assert(noMarkerUpdater.isEmpty)

            Seq("", "cubf-").foreach { bfId =>
              val (invalidBfId, invalidUpdater) =
                CuBFProbeDiagWiring.resolveProbeWiring(probeExpression(bfId))
              assert(invalidBfId.isEmpty)
              assert(invalidUpdater.isEmpty)
            }
            assert(CuBFDiagPairMetric.probeCacheSize === 0)
          }

          withSqlExecutionId(402L) {
            val (validBfId, validUpdater) =
              CuBFProbeDiagWiring.resolveProbeWiring(probeExpression("cubf-probe-ok"))
            assert(validBfId.contains("cubf-probe-ok"))
            assert(validUpdater.isDefined)
            assert(CuBFDiagPairMetric.probeContains(402L, "cubf-probe-ok"))
          }
        }
      } finally {
        CuBFDiagPairMetric.clearAllForTests()
      }
    }

    private def probeExpression(bfId: String): org.apache.spark.sql.execution.ScalarSubquery = {
      org.apache.spark.sql.execution.ScalarSubquery(
        SubqueryExec("probe", TryReadBFRegistryExec(bfId)),
        NamedExpression.newExprId)
    }

  }
}
