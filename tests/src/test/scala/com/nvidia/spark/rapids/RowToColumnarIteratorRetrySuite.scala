/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.nio.charset.StandardCharsets
import java.util.function.Supplier

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore, MemoryBuffer, ParquetOptions, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.parquet.ParquetCachedBatchSerializer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class RowToColumnarIteratorRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val batchSize = 1 * 1024 * 1024 * 1024

  // RmmSparkRetrySuiteBase dedicates the test thread to this task.
  private val taskId = 1L
  private val intStringSchema = StructType(Seq(
    StructField("i", IntegerType), StructField("s", StringType)))
  private val intArraySchema = StructType(Seq(
    StructField("i", IntegerType), StructField("a", ArrayType(StringType))))
  private val intArrayBinarySchema = StructType(Seq(StructField("i", IntegerType),
    StructField("a", ArrayType(StringType)), StructField("b", BinaryType)))
  private val smallBatchBytes = 64L * 1024
  // The cache build's goal: no size target, so a batch ends only where it has to split.
  private val cacheBuildGoal = TargetSize(Long.MaxValue)
  private val productionLimits = RapidsHostColumnBuilder.PRODUCTION_LIMITS
  // The split case: 2047 values fit a string limit one byte short of 2048 values, so row 2048
  // starts a second batch. Rows 2047 and 2048 both have a null INT in one validity byte, so the
  // rollback of row 2048 must keep row 2047's null.
  private val splitCaseRows = 2300
  private val rowsBeforeLimit = 2047
  // The shared-row sources' values: four fit a 4096-byte limit, and row 5, whose INT is null,
  // crosses it.
  private val sharedValueSize = 1000
  private val sharedNullIntRow = 5
  private val largeHostMemoryKey = "spark.rapids.test.largeHostMemory.enabled"
  private val largeHostMemoryCancel =
    "set SPARK_CONF=spark.rapids.test.largeHostMemory.enabled=true to run; see tests/README.md"
  // Read from SPARK_CONF as SparkSessionHolder applies it: this suite runs no Spark session.
  private val largeHostMemoryEnabled = sys.env.get("SPARK_CONF").exists(_.split(",").exists {
    setting =>
      val keyValue = setting.split("=", 2)
      keyValue.length == 2 && keyValue(0).trim == largeHostMemoryKey &&
        keyValue(1).trim.equalsIgnoreCase("true")
  })

  test("test simple GPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test simple CPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    // Inject CPU OOM after skipping the first few CPU allocations. The skipCount ensures
    // the OOM is thrown at a point where our retry logic can handle it (during row conversion,
    // after builder state has been captured).
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test CPU OOM retry preserves all rows for non-RequireSingleBatch") {
    val totalRows = 10
    val rowIter: Iterator[InternalRow] = (1 to totalRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))
    // Inject a CPU OOM during conversion and verify that retry still produces
    // the complete set of rows when the iterator is allowed to emit multiple batches.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    var totalRowsSeen = 0
    while (row2ColIter.hasNext) {
      Arm.withResource(row2ColIter.next()) { batch =>
        totalRowsSeen += batch.numRows()
      }
    }
    assertResult(totalRows)(totalRowsSeen)
  }

  test("test first-row CPU OOM with TargetSize goal falls back to retry") {
    val totalRows = 10
    val rowIter: Iterator[InternalRow] = (1 to totalRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))
    // skipCount=1 lets the first CPU allocation (data buffer) succeed, then fires OOM on the
    // second (validity buffer), so the row is not committed (rowCount == 0). This exercises
    // the blockUntilMemoryFreed path. skipCount=0 does not work: blockThreadUntilReady() has
    // nothing to spill and re-throws the OOM when no prior allocations exist.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 1)
    var totalRowsSeen = 0
    while (row2ColIter.hasNext) {
      Arm.withResource(row2ColIter.next()) { batch =>
        totalRowsSeen += batch.numRows()
      }
    }
    assertResult(totalRows)(totalRowsSeen)
  }

  test("test first-row CPU OOM with RequireSingleBatch falls back to retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    // skipCount=1: same reasoning as the TargetSize test above — fires on the validity
    // buffer allocation during the first row, keeping rowCount == 0 for the OOM.
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 1)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  // Note: SplitAndRetryOOM with rowCount == 0 is propagated directly (can't split a single
  // row). A dedicated CpuSplitAndRetryOOM test for per-row convert() with rowCount == 0 is not
  // feasible because RMM allocator-level OOM injection cannot reliably target it — it tends to
  // hit builders.tryBuild() instead. The GPU split-and-retry test below verifies propagation.
  // SplitAndRetryOOM with rowCount > 0 (emit-early) is covered by the test below.

  test("test CPU SplitAndRetryOOM emit-early for non-RequireSingleBatch") {
    // Same injection as "test CPU OOM retry preserves all rows" but with SplitAndRetryOOM:
    // skipCount=3 reliably fires inside convertRows after at least one row is committed,
    // triggering the emit-early path (rowCount > 0, non-RequireSingleBatch).
    val totalRows = 10
    val rowIter: Iterator[InternalRow] = (1 to totalRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    var totalRowsSeen = 0
    while (row2ColIter.hasNext) {
      Arm.withResource(row2ColIter.next()) { batch =>
        totalRowsSeen += batch.numRows()
      }
    }
    assertResult(totalRows)(totalRowsSeen)
  }

  test("test simple OOM split and retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }

  test("an OOM split after an earlier column appended a null keeps null counts equal to masks") {
    // Row 2's string outgrows the one-byte buffer that row 1 sized, so it allocates after the
    // INT column has appended row 2's null. Row 5 has a null string.
    def data(): IntStringRows = new IntStringRows(
      intIsNull = n => n == 2 || n == 4, stringIsNull = _ == 5, valueSize = n => n)
    val input = data()
    val rows = (1 to 5).iterator.map { n =>
      if (n == 2) {
        new StringGetterHookRow(input.values(n), () => RmmSpark.forceRetryOOM(
          RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.CPU.ordinal, 0))
      } else {
        input.row(n)
      }
    }
    val expected = data()
    val nullCounts = ArrayBuffer[List[Long]]()
    val decodedNullCounts = ArrayBuffer[List[Long]]()
    val serializer = new ParquetCachedBatchSerializer
    val iter = r2c(rows, intStringSchema, TargetSize(smallBatchBytes), smallBatchBytes,
      enableRetry = true)
    val sizes = drainBatches(iter) { (batch, firstRow) =>
      nullCounts += assertIntStringBatch(batch, firstRow, expected).toList
      // The same batch as the cache stores it: Parquet-encoded, then decoded.
      serializer.compressColumnarBatchWithParquet(batch, intStringSchema, intStringSchema,
          smallBatchBytes, false).foreach { cached =>
        withResource(Table.readParquet(ParquetOptions.DEFAULT, cached.buffer)) { table =>
          withResource(GpuColumnVector.from(table,
              intStringSchema.fields.map(_.dataType))) { decoded =>
            decodedNullCounts += assertIntStringBatch(decoded, firstRow, expected).toList
          }
        }
      }
    }
    assertResult(Seq(1, 4), "batch sizes")(sizes)
    assertResult(Seq(List(0L, 0L), List(2L, 1L)), "INT and string null counts")(
      nullCounts.toList)
    assertResult(nullCounts.toList, "null counts after Parquet encoding")(
      decodedNullCounts.toList)
  }

  Seq(true, false).foreach { retry =>
    test("a column limit hit while converting a row splits the cache build before that row, " +
        retryMode(retry)) {
      val valueSize = 1024
      val input = splitCaseData(valueSize)
      val rows = (1 to splitCaseRows).iterator.map { n =>
        if (n == rowsBeforeLimit + 1) {
          new StringGetterHookRow(input.values(n),
            () => throw new ColumnLimitExceededException("a test column limit", "a test remedy"))
        } else {
          input.row(n)
        }
      }
      assertSplitAtLimit(r2c(rows, intStringSchema, cacheBuildGoal, smallBatchBytes, retry),
        valueSize)
    }

    test("a lowered string limit splits the cache build before the row that crosses it, " +
        retryMode(retry)) {
      val valueSize = 1024
      withLimits(loweredLimits(maxStringBytes = splitCaseStringLimit(valueSize))) {
        assertSplitAtLimit(r2c(splitCaseData(valueSize).iterator(splitCaseRows),
          intStringSchema, cacheBuildGoal, smallBatchBytes, retry), valueSize)
      }
    }

    test("a lowered string limit under RequireSingleBatch fails with the builder's message, " +
        retryMode(retry)) {
      val valueSize = 1024
      val limit = splitCaseStringLimit(valueSize)
      withLimits(loweredLimits(maxStringBytes = limit)) {
        val iter = r2c(splitCaseData(valueSize).iterator(splitCaseRows), intStringSchema,
          RequireSingleBatch, smallBatchBytes, retry)
        val e = interceptLimit(iter.next())
        assertResult(builderMessage("The string data size in bytes",
          (rowsBeforeLimit + 1L) * valueSize, limit, DType.STRING))(e.getMessage)
      }
    }

    test("a first row whose array alone exceeds a lowered limit fails with the single-row " +
        "message, " + retryMode(retry)) {
      def arrayRow(n: Int, elements: Array[Any]): InternalRow =
        new GenericInternalRow(Array[Any](n, new GenericArrayData(elements)))
      val element = new Array[Byte](1000)
      fillRepeating(element, element.length, 1)
      val rows = Iterator(
        arrayRow(1, Array.fill[Any](5)(UTF8String.fromBytes(element))),
        arrayRow(2, Array[Any](UTF8String.fromString("2"))))
      withLimits(loweredLimits(maxStringBytes = 4096)) {
        val iter = r2c(rows, intArraySchema, cacheBuildGoal, smallBatchBytes, retry)
        val e = interceptLimit(iter.next())
        assertResult(singleRowMessage("The string data size in bytes", 5000, 4096,
          DType.STRING))(e.getMessage)
      }
    }

    Seq(3, 0).foreach { prefixRows =>
      val where = if (prefixRows > 0) "after a legal prefix" else "on the first row"
      test(s"a column limit exception from the input's next() $where passes through " +
          "unchanged, " + retryMode(retry)) {
        val failure =
          new ColumnLimitExceededException("thrown by the input iterator", "a test remedy")
        val input = new FailingInput(new IntStringRows(
          intIsNull = _ => false, stringIsNull = _ => false, valueSize = _ => 8)
          .iterator(prefixRows), failure)
        val iter = r2c(input, intStringSchema, cacheBuildGoal, smallBatchBytes, retry)
        val e = interceptLimit(iter.next())
        assert(e eq failure, s"expected the input's own exception, got: ${e.getMessage}")
        assertResult(prefixRows + 1, "calls to the input's next()")(input.nextCalls)
      }
    }

    test("the cache build splits 1 MiB strings at the real string limit, " +
        retryMode(retry)) {
      assume(largeHostMemoryEnabled, largeHostMemoryCancel)
      assertAssertionsEnabled()
      val valueSize = 1024 * 1024
      assertResult(productionLimits.maxStringBytes)(splitCaseStringLimit(valueSize))
      // The cache build sizes its builders from min(batchSizeBytes, 1 GiB): 1 GiB by default.
      assertSplitAtLimit(r2c(splitCaseData(valueSize).iterator(splitCaseRows), intStringSchema,
        cacheBuildGoal, 1L << 30, retry), valueSize)
    }

    test("a row too large for empty builders after a legal prefix fails with the single-row " +
        "message, " + retryMode(retry)) {
      RmmSpark.getAndResetNumRetryThrow(taskId)
      val valueSize = (n: Int) => if (n == 4) 5000 else sharedValueSize
      val source = sharedUnsafeRows(4, valueSize)
      val expected = sharedIntStringData(valueSize)
      withLimits(loweredLimits(maxStringBytes = 4096)) {
        val iter = r2c(source, intStringSchema, cacheBuildGoal, smallBatchBytes, retry)
        withResource(iter.next()) { batch =>
          assertResult(3, "rows before the row too large for any batch")(batch.numRows())
          assertIntStringBatch(batch, 1, expected)
        }
        val e = interceptLimit(iter.next())
        assertResult(singleRowMessage("The string data size in bytes", 5000, 4096,
          DType.STRING))(e.getMessage)
      }
      // Row 4 came from the source once; its retry in empty builders used the carried copy.
      assertResult(4, "calls to the source's next()")(source.nextCalls)
      assertResult(0, "retry OOMs")(RmmSpark.getAndResetNumRetryThrow(taskId))
    }
  }

  test("TargetSize ends the first batch at the row estimate and later ones at the byte target") {
    val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
    val shortRows = GpuBatchUtils.estimateRowCount(smallBatchBytes,
      GpuBatchUtils.estimateGpuMemory(intStringSchema, sampleRows), sampleRows)
    val longRows = 320
    val numRows = shortRows + longRows
    def data(): IntStringRows = new IntStringRows(
      intIsNull = n => n <= shortRows && n % 3 == 0,
      stringIsNull = n => n <= shortRows && n % 3 == 0,
      valueSize = n => if (n <= shortRows) 0 else 1024)
    val rowBytes = converterBytes(intStringSchema, data().iterator(numRows), numRows)
    val bytesCut = math.ceil(smallBatchBytes.toDouble / rowBytes.last).toInt
    assert(longRows % bytesCut == 0, s"$longRows long rows do not fill batches of $bytesCut")
    val sizesByMode = Seq(true, false).map { retry =>
      val expected = data()
      val iter = r2c(data().iterator(numRows), intStringSchema, TargetSize(smallBatchBytes),
        smallBatchBytes, retry)
      val cuts = drainCheckingCuts(iter, intStringSchema, smallBatchBytes, rowBytes) {
        (batch, firstRow) => assertIntStringBatch(batch, firstRow, expected)
      }
      val first = cuts.head
      assertResult(shortRows, "rows in the first batch")(first.rows)
      assertResult(first.targetRows, "the first batch ends at the row estimate")(first.rows)
      assert(first.bytes < smallBatchBytes.toDouble, "the first batch is under the byte target")
      cuts.tail.foreach { cut =>
        assertResult(bytesCut, "rows in a batch of long rows")(cut.rows)
        assert(cut.rows < cut.targetRows && cut.bytes >= smallBatchBytes.toDouble,
          s"a batch of long rows must end at the byte target: $cut")
      }
      cuts.map(_.rows)
    }
    assertResult(sizesByMode.head, "batches with r2c retry on against off")(sizesByMode(1))
  }

  test("TargetSize admits the row that crosses the byte target, then ends the batch") {
    val numRows = 200
    def data(): IntStringRows = new IntStringRows(
      intIsNull = _ => false, stringIsNull = _ => false, valueSize = _ => 1024)
    val rowBytes = converterBytes(intStringSchema, data().iterator(numRows), numRows)
    assert(rowBytes.forall(_ == rowBytes.head), "every row has the same converter bytes")
    // 64 rows reach the lower target exactly, so no 65th row is admitted. One byte higher, the
    // batch is still under the target after 64 rows, so the 65th is admitted and ends it.
    val lowerTarget = (64 * rowBytes.head).toLong
    assert(lowerTarget.toDouble == 64 * rowBytes.head, s"64 rows of ${rowBytes.head} bytes")
    Seq(lowerTarget -> 64, (lowerTarget + 1) -> 65).foreach { case (target, fullBatchRows) =>
      val sizesByMode = Seq(true, false).map { retry =>
        val expected = data()
        val iter = r2c(data().iterator(numRows), intStringSchema, TargetSize(target), target,
          retry)
        val cuts = drainCheckingCuts(iter, intStringSchema, target, rowBytes) {
          (batch, firstRow) => assertIntStringBatch(batch, firstRow, expected)
        }
        val sizes = cuts.map(_.rows)
        assert(sizes.init.forall(_ == fullBatchRows), s"batches $sizes at a target of $target")
        sizes
      }
      assertResult(sizesByMode.head, s"batches at a target of $target, retry on against off")(
        sizesByMode(1))
    }
  }

  test("TargetSize gives a row larger than the byte target a batch of its own") {
    val numRows = 6
    def data(): IntStringRows = new IntStringRows(intIsNull = _ => false,
      stringIsNull = _ => false, valueSize = n => if (n == 1) 128 * 1024 else 1024)
    val rowBytes = converterBytes(intStringSchema, data().iterator(numRows), numRows)
    val sizesByMode = Seq(true, false).map { retry =>
      val expected = data()
      val iter = r2c(data().iterator(numRows), intStringSchema, TargetSize(smallBatchBytes),
        smallBatchBytes, retry)
      val cuts = drainCheckingCuts(iter, intStringSchema, smallBatchBytes, rowBytes) {
        (batch, firstRow) => assertIntStringBatch(batch, firstRow, expected)
      }
      assertResult(1, "rows in the batch of the 128 KiB row")(cuts.head.rows)
      cuts.map(_.rows)
    }
    assertResult(sizesByMode.head, "batches with r2c retry on against off")(sizesByMode(1))
  }

  test("a lowered string limit splits shared UnsafeRow input under a finite target") {
    val sizesByMode = Seq(true, false).map { retry =>
      val source = sharedUnsafeRows(10, _ => sharedValueSize)
      val expected = sharedIntStringData(_ => sharedValueSize)
      val intNullCounts = ArrayBuffer[Long]()
      val sizes = withLimits(loweredLimits(maxStringBytes = 4096, maxFixedWidthElements = 4096)) {
        drainBatches(r2c(source, intStringSchema, TargetSize(smallBatchBytes), smallBatchBytes,
          retry)) { (batch, firstRow) =>
          intNullCounts += assertIntStringBatch(batch, firstRow, expected)(0)
        }
      }
      assertSharedRowSplit(sizes, intNullCounts.toList, source)
      sizes
    }
    assertResult(sizesByMode.head, "batches with r2c retry on against off")(sizesByMode(1))
  }

  test("a lowered binary limit splits shared GenericInternalRow input under a finite target") {
    val sizesByMode = Seq(true, false).map { retry =>
      val source = sharedGenericRows(numRows = 10)
      val intNullCounts = ArrayBuffer[Long]()
      val sizes = withLimits(loweredLimits(maxStringBytes = 4096, maxFixedWidthElements = 4096)) {
        drainBatches(r2c(source, intArrayBinarySchema, TargetSize(smallBatchBytes),
          smallBatchBytes, retry)) { (batch, firstRow) =>
          intNullCounts += assertIntArrayBinaryBatch(batch, firstRow)
        }
      }
      assertSharedRowSplit(sizes, intNullCounts.toList, source)
      sizes
    }
    assertResult(sizesByMode.head, "batches with r2c retry on against off")(sizesByMode(1))
  }

  private def retryMode(enabled: Boolean): String =
    if (enabled) "r2c retry on" else "r2c retry off"

  private def r2c(
      rows: Iterator[InternalRow],
      rowSchema: StructType,
      goal: CoalesceSizeGoal,
      batchSizeBytes: Long,
      enableRetry: Boolean): RowToColumnarIterator =
    new RowToColumnarIterator(rows, rowSchema, goal, batchSizeBytes,
      new GpuRowToColumnConverter(rowSchema), enableRetry)

  /** Runs body with lowered limits for the builders this thread creates inside it. */
  private def withLimits[T](limits: RapidsHostColumnBuilder.Limits)(body: => T): T =
    RapidsHostColumnBuilder.withTestLimits[T](limits, new Supplier[T] {
      override def get(): T = body
    })

  private def loweredLimits(
      maxStringBytes: Long,
      maxFixedWidthElements: Long = productionLimits.maxFixedWidthElements) =
    new RapidsHostColumnBuilder.Limits(maxStringBytes, maxFixedWidthElements,
      productionLimits.maxOffsetRows, productionLimits.maxStructRows)

  private def limitDetail(what: String, attempted: Long, limit: Long, dType: DType): String =
    s"$what would be $attempted, exceeding the limit of $limit for a column of cuDF type $dType"

  private def builderMessage(what: String, attempted: Long, limit: Long, dType: DType): String =
    limitDetail(what, attempted, limit, dType) + "; split the input into smaller batches or " +
      "partitions, or reduce the size of individual values"

  private def singleRowMessage(what: String, attempted: Long, limit: Long,
      dType: DType): String =
    "A single row cannot fit in a batch on its own: " +
      limitDetail(what, attempted, limit, dType) + "; reduce the size of that row's values"

  /** Intercepts the limit exception, closing any batch the call returns instead. */
  private def interceptLimit(nextBatch: => ColumnarBatch): ColumnLimitExceededException = {
    val e = intercept[ColumnLimitExceededException] {
      withResource(nextBatch)(_ => ())
    }
    assertResult(classOf[ColumnLimitExceededException])(e.getClass)
    e
  }

  /**
   * The large tests need -ea in the executing JVM, so that an overflow fails on an assertion
   * instead of writing outside a host buffer.
   */
  private def assertAssertionsEnabled(): Unit = {
    assert(classOf[RapidsHostColumnBuilder].desiredAssertionStatus(),
      "run with -ea: assertions are disabled for RapidsHostColumnBuilder")
    assert(classOf[MemoryBuffer].desiredAssertionStatus(),
      "run with -ea: assertions are disabled for cuDF MemoryBuffer")
  }

  private def splitCaseData(valueSize: Int): IntStringRows = new IntStringRows(
    intIsNull = n => n % 10 == 7 || n == rowsBeforeLimit + 1,
    stringIsNull = _ => false,
    valueSize = _ => valueSize)

  private def splitCaseStringLimit(valueSize: Int): Long = (rowsBeforeLimit + 1L) * valueSize - 1

  /**
   * Drains the split case and checks that it ends the first batch before row 2048: two batches,
   * every value equal to the input, every null count equal to its mask, and no retry OOM, so
   * the split never waited in blockUntilMemoryFreed.
   */
  private def assertSplitAtLimit(iter: Iterator[ColumnarBatch], valueSize: Int): Unit = {
    RmmSpark.getAndResetNumRetryThrow(taskId)
    val expected = splitCaseData(valueSize)
    val sizes = drainBatches(iter) { (batch, firstRow) =>
      assertIntStringBatch(batch, firstRow, expected)
    }
    assertResult(Seq(rowsBeforeLimit, splitCaseRows - rowsBeforeLimit), "batch sizes")(sizes)
    assertResult(0, "retry OOMs")(RmmSpark.getAndResetNumRetryThrow(taskId))
  }

  private def sharedIntStringData(valueSize: Int => Int): IntStringRows = new IntStringRows(
    intIsNull = _ == sharedNullIntRow, stringIsNull = _ => false, valueSize = valueSize)

  /** A source that rewrites one UnsafeRow, through an UnsafeProjection, for every row. */
  private def sharedUnsafeRows(numRows: Int, valueSize: Int => Int): SharedRowSource = {
    val data = sharedIntStringData(valueSize)
    val projection = UnsafeProjection.create(intStringSchema)
    new SharedRowSource(numRows, n => projection(data.row(n)))
  }

  /**
   * A source that rewrites one GenericInternalRow for every row, with its BINARY value filled
   * into one reused array, so only a copy of the row keeps a carried row's bytes.
   */
  private def sharedGenericRows(numRows: Int): SharedRowSource = {
    val row = new GenericInternalRow(3)
    val binary = new Array[Byte](sharedValueSize)
    new SharedRowSource(numRows, n => {
      if (n == sharedNullIntRow) row.setNullAt(0) else row.setInt(0, n)
      val digits = UTF8String.fromString(n.toString)
      row.update(1, new GenericArrayData(Array[Any](digits, digits)))
      fillRepeating(binary, binary.length, n)
      row.update(2, binary)
      row
    })
  }

  /**
   * Four shared rows fit the lowered limit, so the batches hold 4, 4 and 2 rows, and row 5's
   * INT null is counted in the second batch only. The source is asked for each row once, so the
   * carried row 5 was converted before any later input row was fetched into the shared row.
   */
  private def assertSharedRowSplit(
      sizes: Seq[Int],
      intNullCounts: Seq[Long],
      source: SharedRowSource): Unit = {
    assertResult(Seq(4, 4, 2), "batch sizes")(sizes)
    assertResult(Seq(0L, 1L, 0L), "INT null counts")(intNullCounts)
    assertResult(10, "calls to the source's next()")(source.nextCalls)
  }

  /** Drains the iterator, checking each batch with its first input row; returns the sizes. */
  private def drainBatches(iter: Iterator[ColumnarBatch])(
      check: (ColumnarBatch, Int) => Unit): Seq[Int] = {
    val sizes = ArrayBuffer[Int]()
    var firstRow = 1
    while (iter.hasNext) {
      withResource(iter.next()) { batch =>
        check(batch, firstRow)
        sizes += batch.numRows()
        firstRow += batch.numRows()
      }
    }
    sizes.toList
  }

  /** A batch as RowToColumnarIterator's own formulas cut it. */
  private case class Cut(rows: Int, bytes: Double, targetRows: Int)

  /**
   * Drains a TargetSize iterator, checking each batch's size against the cut its formulas give:
   * a row target estimated from the schema, then from the device size of the batches emitted so
   * far, and a byte target on the converters' byte counts. A row is admitted while the batch is
   * under both targets, and the first row always is.
   */
  private def drainCheckingCuts(
      iter: Iterator[ColumnarBatch],
      rowSchema: StructType,
      targetBytes: Long,
      rowBytes: Array[Double])(check: (ColumnarBatch, Int) => Unit): Seq[Cut] = {
    val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
    var targetRows = GpuBatchUtils.estimateRowCount(targetBytes,
      GpuBatchUtils.estimateGpuMemory(rowSchema, sampleRows), sampleRows)
    var deviceBytes = 0L
    var start = 0
    val cuts = ArrayBuffer[Cut]()
    while (iter.hasNext) {
      var rows = 0
      var bytes = 0.0
      while (start + rows < rowBytes.length &&
          (rows == 0 || rows < targetRows && bytes < targetBytes.toDouble)) {
        bytes += rowBytes(start + rows)
        rows += 1
      }
      withResource(iter.next()) { batch =>
        assertResult(rows, s"rows in the batch that starts at input row ${start + 1}")(
          batch.numRows())
        check(batch, start + 1)
        deviceBytes += GpuColumnVector.getTotalDeviceMemoryUsed(batch)
      }
      cuts += Cut(rows, bytes, targetRows)
      start += rows
      if (deviceBytes > 0) {
        targetRows = GpuBatchUtils.estimateRowCount(targetBytes, deviceBytes, start)
      }
    }
    assertResult(rowBytes.length, "input rows in all batches")(start)
    cuts.toList
  }

  /** The bytes RowToColumnarIterator counts for each row, from the converters themselves. */
  private def converterBytes(
      rowSchema: StructType,
      rows: Iterator[InternalRow],
      numRows: Int): Array[Double] = {
    val converter = new GpuRowToColumnConverter(rowSchema)
    withResource(new GpuColumnarBatchBuilder(rowSchema, numRows)) { builders =>
      rows.map(row => converter.convert(row, builders)).toArray
    }
  }

  private def withHostColumns[T](batch: ColumnarBatch)(body: Array[HostColumnVector] => T): T =
    withResource(GpuColumnVector.extractBases(batch).safeMap(_.copyToHost()))(body)

  /** A column's null count must equal the nulls in its validity mask, as must its children's. */
  private def assertNullCountMatchesMask(column: HostColumnVectorCore): Unit = {
    var maskNulls = 0L
    var i = 0L
    while (i < column.getRowCount) {
      if (column.isNull(i)) {
        maskNulls += 1
      }
      i += 1
    }
    assertResult(maskNulls, s"null count of a ${column.getType} column against its mask")(
      column.getNullCount)
    (0 until column.getNumChildren).foreach { c =>
      assertNullCountMatchesMask(column.getChildColumnView(c))
    }
  }

  /**
   * Checks a batch of the INT and STRING schema against the expected rows from firstRow on, and
   * each column's null count against its mask. Returns the columns' null counts.
   */
  private def assertIntStringBatch(
      batch: ColumnarBatch,
      firstRow: Int,
      expected: IntStringRows): Array[Long] = {
    withHostColumns(batch) { columns =>
      columns.foreach(column => assertNullCountMatchesMask(column))
      val ints = columns(0)
      val strings = columns(1)
      (0 until batch.numRows()).foreach { i =>
        val n = firstRow + i
        val row = expected.row(n)
        assertResult(row.isNullAt(0), s"INT null at row $n")(ints.isNull(i))
        if (!row.isNullAt(0)) {
          assertResult(row.getInt(0), s"INT at row $n")(ints.getInt(i))
        }
        assertResult(row.isNullAt(1), s"string null at row $n")(strings.isNull(i))
        if (!row.isNullAt(1)) {
          // Compared as a flag, so that a failure does not print values of up to 1 MiB.
          val equal = UTF8String.fromBytes(strings.getUTF8(i)) == row.getUTF8String(1)
          assert(equal, s"string at row $n")
        }
      }
      columns.map(_.getNullCount)
    }
  }

  /** Checks a batch of the shared GenericInternalRow source; returns the INT null count. */
  private def assertIntArrayBinaryBatch(batch: ColumnarBatch, firstRow: Int): Long = {
    val expectedBinary = new Array[Byte](sharedValueSize)
    withHostColumns(batch) { columns =>
      columns.foreach(column => assertNullCountMatchesMask(column))
      val ints = columns(0)
      val arrays = columns(1)
      val elements = arrays.getChildColumnView(0)
      val binaries = columns(2)
      (0 until batch.numRows()).foreach { i =>
        val n = firstRow + i
        assertResult(n == sharedNullIntRow, s"INT null at row $n")(ints.isNull(i))
        if (n != sharedNullIntRow) {
          assertResult(n, s"INT at row $n")(ints.getInt(i))
        }
        val array = (arrays.getStartListOffset(i) until arrays.getEndListOffset(i))
          .map(j => elements.getJavaString(j))
        assertResult(Seq(n.toString, n.toString), s"array at row $n")(array)
        fillRepeating(expectedBinary, expectedBinary.length, n)
        val equal = java.util.Arrays.equals(binaries.getBytesFromList(i), expectedBinary)
        assert(equal, s"binary at row $n")
      }
      ints.getNullCount
    }
  }

  /** Writes the ASCII digits of n, repeated, into the first len bytes of buffer. */
  private def fillRepeating(buffer: Array[Byte], len: Int, n: Int): Unit = {
    val digits = n.toString.getBytes(StandardCharsets.US_ASCII)
    var filled = math.min(digits.length, len)
    System.arraycopy(digits, 0, buffer, 0, filled)
    // Each copy starts at a whole number of periods, so doubling keeps the pattern.
    while (filled < len) {
      val chunk = math.min(filled, len - filled)
      System.arraycopy(buffer, 0, buffer, filled, chunk)
      filled += chunk
    }
  }

  /**
   * Rows of a nullable INT and a nullable STRING, numbered from 1: the INT is the row number and
   * the string repeats its digits for valueSize(n) bytes. Strings are written into one reused
   * array, so each row must be consumed before the next one is made, as with Spark's sources.
   */
  private class IntStringRows(
      intIsNull: Int => Boolean,
      stringIsNull: Int => Boolean,
      valueSize: Int => Int) {
    private var buffer = new Array[Byte](0)

    def values(n: Int): Array[Any] = {
      val string = if (stringIsNull(n)) {
        null
      } else {
        val size = valueSize(n)
        if (buffer.length < size) {
          buffer = new Array[Byte](size)
        }
        fillRepeating(buffer, size, n)
        UTF8String.fromBytes(buffer, 0, size)
      }
      Array[Any](if (intIsNull(n)) null else n, string)
    }

    def row(n: Int): InternalRow = new GenericInternalRow(values(n))

    def iterator(numRows: Int): Iterator[InternalRow] = (1 to numRows).iterator.map(n => row(n))
  }

  /** A row whose string getter runs a hook the first time it is called. */
  private class StringGetterHookRow(rowValues: Array[Any], hook: () => Unit)
      extends GenericInternalRow(rowValues) {
    private var hookRan = false

    override def getUTF8String(ordinal: Int): UTF8String = {
      if (!hookRan) {
        hookRan = true
        hook()
      }
      super.getUTF8String(ordinal)
    }
  }

  /**
   * Refills one shared row with the next input row in hasNext and returns it from next(), as
   * Spark's BufferedRowIterator does, so a row kept past the next hasNext changes under its
   * holder.
   */
  private class SharedRowSource(numRows: Int, fill: Int => InternalRow)
      extends Iterator[InternalRow] {
    private var produced = 0
    private var current: InternalRow = _
    var nextCalls = 0

    override def hasNext: Boolean = {
      if (current == null && produced < numRows) {
        current = fill(produced + 1)
      }
      current != null
    }

    override def next(): InternalRow = {
      nextCalls += 1
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val row = current
      current = null
      produced += 1
      row
    }
  }

  /** Returns the prefix rows, then throws the given exception from next(). */
  private class FailingInput(prefix: Iterator[InternalRow], failure: RuntimeException)
      extends Iterator[InternalRow] {
    var nextCalls = 0

    override def hasNext: Boolean = true

    override def next(): InternalRow = {
      nextCalls += 1
      if (prefix.hasNext) prefix.next() else throw failure
    }
  }
}
