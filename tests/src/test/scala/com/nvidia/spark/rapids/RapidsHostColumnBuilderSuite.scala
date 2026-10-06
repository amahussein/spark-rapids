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

package com.nvidia.spark.rapids

import java.nio.charset.StandardCharsets
import java.util.function.Supplier

import scala.util.{Failure, Try}

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore}
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType, StructType}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsHostColumnBuilder.Limits
import org.scalatest.funsuite.AnyFunSuite

class RapidsHostColumnBuilderSuite extends AnyFunSuite {
  private val production = RapidsHostColumnBuilder.PRODUCTION_LIMITS

  // Small enough that a test reaches a limit with a few KiB of host memory.
  private val smallLimit = 4096

  // An estimate of 1 makes every buffer grow by doubling; an estimate above the limit makes
  // the first allocation exceed it unless that allocation is bounded too.
  private val estimates = Seq(1L, 2L * smallLimit)

  private val intType = new BasicType(true, DType.INT32)
  private val stringType = new BasicType(true, DType.STRING)
  private val binaryType = new ListType(true, new BasicType(false, DType.UINT8))

  private def limits(
      stringBytes: Long = production.maxStringBytes,
      fixedWidthElements: Long = production.maxFixedWidthElements,
      offsetRows: Long = production.maxOffsetRows,
      structRows: Long = production.maxStructRows): Limits =
    new Limits(stringBytes, fixedWidthElements, offsetRows, structRows)

  private def withLimits[T](testLimits: Limits)(body: => T): T =
    RapidsHostColumnBuilder.withTestLimits(testLimits, new Supplier[T] {
      override def get(): T = body
    })

  private def limitMessage(what: String, attempted: Long, limit: Long, dtype: DType): String =
    s"$what would be $attempted, exceeding the limit of $limit for a column of cuDF type " +
      s"$dtype; split the input into smaller batches or partitions, or reduce the size of " +
      "individual values"

  private def bytesMessage(attempted: Long, limit: Long): String =
    limitMessage("The string data size in bytes", attempted, limit, DType.STRING)

  private def elementsMessage(attempted: Long, limit: Long, dtype: DType): String =
    limitMessage("The number of elements", attempted, limit, dtype)

  private def rowsMessage(attempted: Long, limit: Long, dtype: DType): String =
    limitMessage("The number of rows", attempted, limit, dtype)

  private def assertRejects(expectedMessage: String)(append: => Any): Unit = {
    val e = intercept[ColumnLimitExceededException](append)
    assertResult(expectedMessage)(e.getMessage)
  }

  private def isNullRow(i: Int): Boolean = i % 7 == 0

  private def letters(size: Int): Array[Byte] =
    Array.tabulate[Byte](size)(i => ('a' + i % 26).toByte)

  /** Appends one string of the given size to a new builder; false if a limit rejects it. */
  private def acceptsString(size: Int): Boolean =
    withResource(new RapidsHostColumnBuilder(stringType, 1)) { b =>
      try {
        b.appendUTF8String(new Array[Byte](size))
        true
      } catch {
        case _: ColumnLimitExceededException => false
      }
    }

  private def validityBytes(rows: Long): Long = ((rows + 7) / 8 + 63) / 64 * 64

  /** Checks that no buffer of the column or of its children is longer than the limits allow. */
  private def assertBuffersWithin(col: HostColumnVectorCore, l: Limits): Unit = {
    val dtype = col.getType
    val rowLimit = dtype match {
      case DType.STRING | DType.LIST => l.maxOffsetRows
      case DType.STRUCT => l.maxStructRows
      case _ => l.maxFixedWidthElements
    }
    val dataLimit = dtype match {
      case DType.STRING => l.maxStringBytes
      case _ => l.maxFixedWidthElements * dtype.getSizeInBytes
    }
    Option(col.getData).foreach { data =>
      assert(data.getLength <= dataLimit, s"$dtype data")
    }
    Option(col.getOffsets).foreach { offsets =>
      assert(offsets.getLength <= (rowLimit + 1) * DType.INT32.getSizeInBytes, s"$dtype offsets")
    }
    Option(col.getValidity).foreach { valid =>
      assert(valid.getLength <= validityBytes(rowLimit), s"$dtype validity")
    }
    (0 until col.getNumChildren).foreach(i => assertBuffersWithin(col.getChildColumnView(i), l))
  }

  /** Checks that the null count of the column and of each child matches its validity mask. */
  private def assertNullCountsMatchMasks(col: HostColumnVectorCore): Unit = {
    val maskNulls = (0L until col.getRowCount).count(i => col.isNull(i)).toLong
    assertResult(maskNulls, s"${col.getType} null count")(col.getNullCount)
    (0 until col.getNumChildren).foreach { i =>
      assertNullCountsMatchMasks(col.getChildColumnView(i))
    }
  }

  private def buildAndCheck(b: RapidsHostColumnBuilder, l: Limits)(
      check: HostColumnVector => Unit): Unit =
    withResource(b.build()) { v =>
      check(v)
      assertNullCountsMatchMasks(v)
      assertBuffersWithin(v, l)
    }

  test("growing buffer preserves correctness") {
    val b1 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0) // grows
    val b2 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 8) // does not grow
    for (i <- 0 to 7) {
      b1.append(i)
      b2.append(i)
    }
    val v1 = b1.build()
    val v2 = b2.build()
    for (i <- 0 to 7) {
      assertResult(v1.getInt(i))(v2.getInt(i))
    }
    v1.close()
    v2.close()
    b1.close()
    b2.close()
  }

  test("appendLists walks appendChildOrNull for typed and null list elements") {
    // appendLists -> append(List) -> appendChildOrNull: one arm per element type, plus the null arm
    def buildList(childType: DType, elems: AnyRef*): Unit = {
      val lt = new ListType(true, new BasicType(true, childType))
      val b = new RapidsHostColumnBuilder(lt, 1)
      try {
        b.appendLists(java.util.Arrays.asList(elems: _*))
        val v = b.build()
        try {
          assertResult(1L)(v.getRowCount)
        } finally {
          v.close()
        }
      } finally {
        b.close()
      }
    }
    buildList(DType.INT32, Integer.valueOf(1), null, Integer.valueOf(3))
    buildList(DType.INT64, java.lang.Long.valueOf(1L), null)
    buildList(DType.FLOAT64, java.lang.Double.valueOf(1.0d), null)
    buildList(DType.FLOAT32, java.lang.Float.valueOf(1.0f), null)
    buildList(DType.BOOL8, java.lang.Boolean.TRUE, null)
    buildList(DType.STRING, "a", null)
  }

  test("captureState then restoreState rolls back appended struct rows including children") {
    val st = new StructType(true,
      new BasicType(true, DType.INT32),
      new BasicType(true, DType.INT32))
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).append(1)
      b.getChild(1).append(10)
      b.endStruct()
      val snapshot = b.captureState()
      b.getChild(0).append(2)
      b.getChild(1).append(20)
      b.endStruct()
      b.restoreState(snapshot)
      val v = b.build()
      try {
        assertResult(1L)(v.getRowCount)
      } finally {
        v.close()
      }
    } finally {
      b.close()
    }
  }

  test("restoreState handles non-null rows beyond the allocated validity bitmap") {
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT32), 4)) { b =>
      b.appendNull()
      (1 to 4096).foreach(i => b.append(i))
      val snapshot = b.captureState()
      b.append(4097)
      b.restoreState(snapshot)
      b.append(4098)
      withResource(b.build()) { column =>
        assertResult(4098L)(column.getRowCount)
        assertResult(1L)(column.getNullCount)
        assert(column.isNull(0))
        (1 to 4096).foreach(i => assertResult(i)(column.getInt(i)))
        assertResult(4098)(column.getInt(4097))
      }
    }
  }

  test("restoreState preserves earlier nulls and is idempotent across validity bytes") {
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT32), 16)) { b =>
      b.appendNull()
      (1 to 6).foreach(i => b.append(i))
      val snapshot = b.captureState()
      b.appendNull()
      b.append(8)
      b.appendNull()
      b.restoreState(snapshot)
      b.restoreState(snapshot)
      (7 to 9).foreach(i => b.append(i))
      withResource(b.build()) { column =>
        assertResult(10L)(column.getRowCount)
        assertResult(1L)(column.getNullCount)
        assert(column.isNull(0))
        (1 to 9).foreach { i =>
          assert(!column.isNull(i))
          assertResult(i)(column.getInt(i))
        }
      }
    }
  }

  test("restoreState rolls back a partial struct child before non-null replay") {
    val byteList = new ListType(true, new BasicType(false, DType.UINT8))
    withResource(new RapidsHostColumnBuilder(new StructType(true, byteList, byteList), 4)) {
      b =>
        val snapshot = b.captureState()
        b.getChild(0).appendNull()
        b.restoreState(snapshot)
        b.getChild(0).appendByteList(Array[Byte](1, 2))
        b.getChild(1).appendByteList(Array[Byte](3))
        b.endStruct()
        withResource(b.build()) { column =>
          assertResult(1L)(column.getRowCount)
          assertResult(0L)(column.getNullCount)
          (0 until column.getNumChildren).foreach { index =>
            withResource(column.getChildColumnView(index)) { child =>
              assertResult(0L)(child.getNullCount)
              assert(!child.isNull(0))
              withResource(child.getChildColumnView(0)) { values =>
                val expected = if (index == 0) Array[Byte](1, 2) else Array[Byte](3)
                assertResult(expected.length.toLong)(values.getRowCount)
                expected.indices.foreach(i => assertResult(expected(i))(values.getByte(i)))
              }
            }
          }
        }
    }
  }

  test("restoreState rolls back null counts and validity recursively") {
    val byteList = new ListType(true, new BasicType(false, DType.UINT8))
    val st = new StructType(true, byteList, byteList)
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).appendByteList(Array[Byte](1, 2))
      b.getChild(1).appendByteList(Array[Byte](3))
      b.endStruct()

      val snapshot = b.captureState()
      b.appendNull()
      b.restoreState(snapshot)

      val partial = b.build()
      try {
        assertResult(1L)(partial.getRowCount)
        assertResult(0L)(partial.getNullCount)
        (0 until partial.getNumChildren).foreach { index =>
          withResource(partial.getChildColumnView(index)) { child =>
            assertResult(0L)(child.getNullCount)
          }
        }
      } finally {
        partial.close()
      }

      b.appendNull()
      val replayed = b.build()
      try {
        assertResult(2L)(replayed.getRowCount)
        assertResult(1L)(replayed.getNullCount)
        (0 until replayed.getNumChildren).foreach { index =>
          withResource(replayed.getChildColumnView(index)) { child =>
            assertResult(1L)(child.getNullCount)
          }
        }
      } finally {
        replayed.close()
      }
    } finally {
      b.close()
    }
  }

  test("a rolled-back row leaves no null in the prefix's null count or validity mask") {
    withResource(new RapidsHostColumnBuilder(intType, 4)) { ints =>
      withResource(new RapidsHostColumnBuilder(stringType, 4)) { strings =>
        ints.append(1)
        strings.append("a")
        ints.append(2)
        strings.append("b")
        val intState = ints.captureState()
        val stringState = strings.captureState()
        // The next row's STRING value fails after its INT null went in, so the row is rolled
        // back and moves to the next batch.
        ints.appendNull()
        ints.restoreState(intState)
        strings.restoreState(stringState)
        withResource(ints.build()) { v =>
          assertResult(2L)(v.getRowCount)
          assertResult(0L)(v.getNullCount)
          assertResult(Seq(1, 2))((0 until 2).map(i => v.getInt(i)))
          assertNullCountsMatchMasks(v)
        }
        withResource(strings.build()) { v =>
          assertResult(Seq("a", "b"))((0 until 2).map(i => v.getJavaString(i)))
          assertNullCountsMatchMasks(v)
        }
      }
    }
  }

  // A control: replay into the same builder already counted each null once, and the restore
  // of the null count must not change that.
  test("replaying a rolled-back row into the same builders counts each of its nulls once") {
    withResource(new RapidsHostColumnBuilder(intType, 4)) { ints =>
      withResource(new RapidsHostColumnBuilder(stringType, 4)) { strings =>
        ints.append(1)
        strings.appendNull()
        val intState = ints.captureState()
        val stringState = strings.captureState()
        // The row of two nulls fails twice before it goes in: first in its STRING column, then
        // after both columns took it.
        ints.appendNull()
        ints.restoreState(intState)
        strings.restoreState(stringState)
        ints.appendNull()
        strings.appendNull()
        ints.restoreState(intState)
        strings.restoreState(stringState)
        ints.appendNull()
        strings.appendNull()
        ints.append(3)
        strings.append("c")
        withResource(ints.build()) { v =>
          assertResult(1L)(v.getNullCount)
          assertResult(Seq(false, true, false))((0 until 3).map(i => v.isNull(i)))
          assertResult(3)(v.getInt(2))
          assertNullCountsMatchMasks(v)
        }
        withResource(strings.build()) { v =>
          assertResult(2L)(v.getNullCount)
          assertResult(Seq(true, true, false))((0 until 3).map(i => v.isNull(i)))
          assertResult("c")(v.getJavaString(2))
          assertNullCountsMatchMasks(v)
        }
      }
    }
  }

  test("restoring a struct or a list rolls back the nulls its children took") {
    withResource(new RapidsHostColumnBuilder(new StructType(true, intType, stringType), 4)) { b =>
      b.getChild(0).append(1)
      b.getChild(1).append("a")
      b.endStruct()
      val state = b.captureState()
      // A non-null struct with a null child, then a null struct
      b.getChild(0).appendNull()
      b.getChild(1).append("b")
      b.endStruct()
      b.appendNull()
      b.restoreState(state)
      withResource(b.build()) { v =>
        assertResult(1L)(v.getRowCount)
        assertResult(0L)(v.getNullCount)
        assertResult(0L)(v.getChildColumnView(0).getNullCount)
        assertResult(0L)(v.getChildColumnView(1).getNullCount)
        assertResult(1)(v.getChildColumnView(0).getInt(0))
        assertResult("a")(v.getChildColumnView(1).getJavaString(0))
        assertNullCountsMatchMasks(v)
      }
    }
    withResource(new RapidsHostColumnBuilder(new ListType(true, intType), 4)) { b =>
      b.getChild(0).append(1)
      b.endList()
      val state = b.captureState()
      b.getChild(0).appendNull()
      b.getChild(0).append(2)
      b.endList()
      b.restoreState(state)
      withResource(b.build()) { v =>
        assertResult(1L)(v.getRowCount)
        val child = v.getChildColumnView(0)
        assertResult(1L)(child.getRowCount)
        assertResult(0L)(child.getNullCount)
        assertResult(1)(child.getInt(0))
        assertNullCountsMatchMasks(v)
      }
    }
  }

  test("restoring a list resets only its child's discarded validity bits, within the buffer") {
    assume(classOf[RapidsHostColumnBuilder].desiredAssertionStatus(), "needs -ea")
    // The child's first null allocates 512 validity bits, which the discarded row outgrows
    // with 517 elements; a reset past the buffer fails cuDF's bounds check under -ea.
    val listOfInts = new ListType(true, intType)
    val firstValues = Seq(1, 2)
    val secondValues = 10 until 10 + 513
    def appendRow(b: RapidsHostColumnBuilder, values: Seq[Int]): Long = {
      val validityGrowth = b.getChild(0).appendNull()
      values.foreach(i => b.getChild(0).append(i))
      b.endList()
      validityGrowth
    }
    def appendAndRestore(b: RapidsHostColumnBuilder): Unit = {
      assertResult(64L)(appendRow(b, firstValues))
      val state = b.captureState()
      appendRow(b, secondValues)
      assertResult(517)(b.getChild(0).getCurrentIndex)
      b.restoreState(state)
    }
    withResource(new RapidsHostColumnBuilder(listOfInts, 1)) { b =>
      appendAndRestore(b)
      withResource(b.build()) { v =>
        assertResult(1L)(v.getRowCount)
        val child = v.getChildColumnView(0)
        assertResult(3L)(child.getRowCount)
        assertResult(1L)(child.getNullCount)
        assertResult(Seq(0L))((0L until child.getRowCount).filter(i => child.isNull(i)))
        assertResult(firstValues)(Seq(child.getInt(1), child.getInt(2)))
        assertNullCountsMatchMasks(v)
      }
    }
    withResource(new RapidsHostColumnBuilder(listOfInts, 1)) { b =>
      appendAndRestore(b)
      appendRow(b, secondValues)
      withResource(b.build()) { v =>
        assertResult(2L)(v.getRowCount)
        val child = v.getChildColumnView(0)
        assertResult(517L)(child.getRowCount)
        assertResult(2L)(child.getNullCount)
        assertResult(Seq(0L, 3L))((0L until child.getRowCount).filter(i => child.isNull(i)))
        secondValues.indices.foreach { i =>
          assertResult(secondValues(i))(child.getInt(4 + i))
        }
        assertNullCountsMatchMasks(v)
      }
    }
  }

  test("appendUTF8String appends a valid UTF-8 subrange of a larger array") {
    // The inverted check this guards against is an assert.
    assume(classOf[RapidsHostColumnBuilder].desiredAssertionStatus(), "needs -ea")
    // 0xE9 is e-acute, two bytes in UTF-8, so the byte offsets differ from the char offsets.
    val text = "xxcaf" + 0xE9.toChar + "yy"
    val bytes = text.getBytes(StandardCharsets.UTF_8)
    def byteLength(s: String): Int = s.getBytes(StandardCharsets.UTF_8).length
    val inside = text.substring(2, 6)
    val atEnd = text.substring(4)
    withResource(new RapidsHostColumnBuilder(stringType, 2)) { b =>
      b.appendUTF8String(bytes, byteLength(text.substring(0, 2)), byteLength(inside))
      b.appendUTF8String(bytes, byteLength(text.substring(0, 4)), byteLength(atEnd))
      withResource(b.build()) { v =>
        assertResult(inside)(v.getJavaString(0))
        assertResult(atEnd)(v.getJavaString(1))
        assert(v.getUTF8(0).sameElements(inside.getBytes(StandardCharsets.UTF_8)))
        assert(v.getUTF8(1).sameElements(atEnd.getBytes(StandardCharsets.UTF_8)))
      }
    }
  }

  test("appendUTF8String asserts on a subrange past the array before changing the builder") {
    // The subrange check is a Java assertion, so it only runs under -ea.
    assume(classOf[RapidsHostColumnBuilder].desiredAssertionStatus(), "needs -ea")
    val bytes = "abcd".getBytes(StandardCharsets.UTF_8)
    withResource(new RapidsHostColumnBuilder(stringType, 2)) { b =>
      b.appendUTF8String(bytes, 0, 2)
      // srcOffset + length overflows Int, so only a check that subtracts rejects it.
      intercept[AssertionError](b.appendUTF8String(bytes, 1, Int.MaxValue))
      b.appendUTF8String(bytes, 2, 2)
      withResource(b.build()) { v =>
        assertResult(2L)(v.getRowCount)
        assertResult(Seq("ab", "cd"))((0 until 2).map(i => v.getJavaString(i)))
      }
    }
  }

  test("a string column rejects the value that would cross its byte limit and keeps its rows") {
    // One below a power of two, like the production limit of 2^31 - 1
    val limit = smallLimit - 1
    val testLimits = limits(stringBytes = limit)
    // A power-of-two value size, and one that makes the buffer double unevenly
    Seq(64, 61).foreach { valueSize =>
      val value = letters(valueSize)
      val fullRows = limit / valueSize
      val rest = limit - fullRows * valueSize
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(stringType, 1)) { b =>
          (0 until fullRows).foreach(_ => b.appendUTF8String(value))
          assertRejects(bytesMessage((fullRows + 1L) * valueSize, limit)) {
            b.appendUTF8String(value)
          }
          b.appendUTF8String(value, 0, rest)
          assertRejects(bytesMessage(limit + 1L, limit))(b.appendUTF8String(value, 0, 1))
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            (0 until fullRows).foreach(i => assert(v.getUTF8(i).sameElements(value)))
            assert(v.getUTF8(fullRows).sameElements(value.take(rest)))
            assertResult(limit.toLong)(v.getEndListOffset(fullRows))
          }
        }
      }
    }
  }

  test("a string column takes a value just under its byte limit, then one exactly to it") {
    val testLimits = limits(stringBytes = smallLimit)
    withLimits(testLimits) {
      withResource(new RapidsHostColumnBuilder(stringType, 1)) { b =>
        b.appendUTF8String(new Array[Byte](smallLimit - 8))
        b.appendUTF8String(new Array[Byte](8))
        assertRejects(bytesMessage(smallLimit + 1L, smallLimit)) {
          b.appendUTF8String(new Array[Byte](1))
        }
        buildAndCheck(b, testLimits) { v =>
          assertResult(Seq(smallLimit - 8, 8))((0 until 2).map(i => v.getUTF8(i).length))
          assertResult(smallLimit.toLong)(v.getData.getLength)
        }
      }
    }
  }

  test("a binary column rejects the value that would cross its element limit and keeps its rows") {
    // Two below a power of two, like the production limit of 2^31 - 2
    val limit = smallLimit - 2
    val testLimits = limits(fixedWidthElements = limit)
    val value = Array.tabulate[Byte](64)(_.toByte)
    val fullRows = limit / value.length
    val rest = limit - fullRows * value.length
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(binaryType, estimate)) { b =>
          (0 until fullRows).foreach(_ => b.appendByteList(value))
          assertRejects(elementsMessage((fullRows + 1L) * value.length, limit, DType.UINT8)) {
            b.appendByteList(value)
          }
          b.appendByteList(value, 0, rest)
          assertRejects(elementsMessage(limit + 1L, limit, DType.UINT8)) {
            b.appendByteList(value, 0, 1)
          }
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            assertResult(limit.toLong)(v.getChildColumnView(0).getRowCount)
            (0 until fullRows).foreach(i => assert(v.getBytesFromList(i).sameElements(value)))
            assert(v.getBytesFromList(fullRows).sameElements(value.take(rest)))
          }
        }
      }
    }
  }

  test("a LIST<STRING> column rejects in its STRING child and rolls back to a valid prefix") {
    val limit = smallLimit - 1
    val testLimits = limits(stringBytes = limit)
    val value = letters(64)
    val valuesPerRow = 2
    val fullRows = limit / (valuesPerRow * value.length)
    withLimits(testLimits) {
      withResource(new RapidsHostColumnBuilder(new ListType(true, stringType), 1)) { b =>
        val strings = b.getChild(0)
        def appendRow(): Unit = {
          (0 until valuesPerRow).foreach(_ => strings.appendUTF8String(value))
          b.endList()
        }
        (0 until fullRows).foreach(_ => appendRow())
        val state = b.captureState()
        assertRejects(bytesMessage((fullRows + 1L) * valuesPerRow * value.length, limit)) {
          appendRow()
        }
        // The row's first string went in before its second one was rejected.
        assertResult(fullRows * valuesPerRow + 1)(strings.getCurrentIndex)
        b.restoreState(state)
        buildAndCheck(b, testLimits) { v =>
          assertResult(fullRows.toLong)(v.getRowCount)
          val child = v.getChildColumnView(0)
          assertResult(fullRows.toLong * valuesPerRow)(child.getRowCount)
          (0 until fullRows * valuesPerRow).foreach { i =>
            assert(child.getUTF8(i).sameElements(value))
          }
        }
      }
    }
  }

  test("a row rejected by the second of two string columns rolls back in both") {
    val testLimits = limits(stringBytes = smallLimit)
    val value = letters(64)
    val fullRows = smallLimit / value.length
    withLimits(testLimits) {
      withResource(new RapidsHostColumnBuilder(stringType, 1)) { shortValues =>
        withResource(new RapidsHostColumnBuilder(stringType, 1)) { longValues =>
          def appendRow(i: Int): Unit = {
            shortValues.append(s"r$i")
            longValues.appendUTF8String(value)
          }
          (0 until fullRows).foreach(i => appendRow(i))
          val shortState = shortValues.captureState()
          val longState = longValues.captureState()
          assertRejects(bytesMessage(smallLimit + value.length.toLong, smallLimit)) {
            appendRow(fullRows)
          }
          // The first column took the row before the second one rejected it.
          assertResult(fullRows + 1)(shortValues.getCurrentIndex)
          assertResult(fullRows)(longValues.getCurrentIndex)
          shortValues.restoreState(shortState)
          longValues.restoreState(longState)
          buildAndCheck(shortValues, testLimits) { v =>
            assertResult(fullRows.toLong)(v.getRowCount)
            assertResult(s"r${fullRows - 1}")(v.getJavaString(fullRows - 1))
          }
          buildAndCheck(longValues, testLimits) { v =>
            assertResult(fullRows.toLong)(v.getRowCount)
            assertResult(smallLimit.toLong)(v.getEndListOffset(fullRows - 1))
          }
        }
      }
    }
  }

  test("a fixed-width column accepts elements up to its limit and rejects one more") {
    val testLimits = limits(fixedWidthElements = smallLimit)
    val expected = elementsMessage(smallLimit + 1L, smallLimit, DType.INT64)
    val longType = new BasicType(true, DType.INT64)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(longType, estimate)) { b =>
          (0 until smallLimit).foreach { i =>
            if (isNullRow(i)) b.appendNull() else b.append(i.toLong)
          }
          assertRejects(expected)(b.append(-1L))
          assertRejects(expected)(b.appendNull())
          buildAndCheck(b, testLimits) { v =>
            assertResult(smallLimit.toLong)(v.getRowCount)
            (0 until smallLimit).foreach { i =>
              if (isNullRow(i)) assert(v.isNull(i)) else assertResult(i.toLong)(v.getLong(i))
            }
          }
        }
      }
    }
  }

  test("a string column accepts rows up to its row limit and rejects one more") {
    val testLimits = limits(offsetRows = smallLimit)
    val expected = rowsMessage(smallLimit + 1L, smallLimit, DType.STRING)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(stringType, estimate)) { b =>
          (0 until smallLimit).foreach { i =>
            if (isNullRow(i)) b.appendNull() else b.append(i.toString)
          }
          assertRejects(expected)(b.append(""))
          assertRejects(expected)(b.appendNull())
          buildAndCheck(b, testLimits) { v =>
            assertResult(smallLimit.toLong)(v.getRowCount)
            (0 until smallLimit).foreach { i =>
              if (isNullRow(i)) {
                assert(v.isNull(i))
              } else {
                assertResult(i.toString)(v.getJavaString(i))
              }
            }
          }
        }
      }
    }
  }

  test("a list column accepts rows up to its row limit and rejects one more") {
    val testLimits = limits(offsetRows = smallLimit)
    val expected = rowsMessage(smallLimit + 1L, smallLimit, DType.LIST)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(new ListType(true, intType), estimate)) { b =>
          (0 until smallLimit).foreach { i =>
            if (isNullRow(i)) {
              b.appendNull()
            } else {
              b.getChild(0).append(i)
              b.endList()
            }
          }
          assertRejects(expected)(b.endList())
          assertRejects(expected)(b.appendNull())
          buildAndCheck(b, testLimits) { v =>
            assertResult(smallLimit.toLong)(v.getRowCount)
            val child = v.getChildColumnView(0)
            (0 until smallLimit).foreach { i =>
              if (isNullRow(i)) {
                assert(v.isNull(i))
              } else {
                assertResult(1L)(v.getEndListOffset(i) - v.getStartListOffset(i))
                assertResult(i)(child.getInt(v.getStartListOffset(i)))
              }
            }
          }
        }
      }
    }
  }

  test("a struct column accepts rows up to its row limit and rejects one more") {
    val testLimits = limits(structRows = smallLimit)
    val expected = rowsMessage(smallLimit + 1L, smallLimit, DType.STRUCT)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(new StructType(true, intType), estimate)) { b =>
          (0 until smallLimit).foreach { i =>
            if (isNullRow(i)) {
              b.appendNull()
            } else {
              b.getChild(0).append(i)
              b.endStruct()
            }
          }
          assertRejects(expected)(b.endStruct())
          assertRejects(expected)(b.appendNull())
          buildAndCheck(b, testLimits) { v =>
            assertResult(smallLimit.toLong)(v.getRowCount)
            val child = v.getChildColumnView(0)
            assertResult(smallLimit.toLong)(child.getRowCount)
            (0 until smallLimit).foreach { i =>
              if (isNullRow(i)) assert(v.isNull(i)) else assertResult(i)(child.getInt(i))
            }
          }
        }
      }
    }
  }

  test("an ARRAY<BINARY> column limits the bytes of its binary values") {
    val testLimits = limits(fixedWidthElements = smallLimit)
    val value = Array.tabulate[Byte](64)(_.toByte)
    val valuesPerRow = 2
    val fullRows = smallLimit / (valuesPerRow * value.length)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(new ListType(true, binaryType), estimate)) { b =>
          val binaries = b.getChild(0)
          (0 until fullRows).foreach { _ =>
            (0 until valuesPerRow).foreach(_ => binaries.appendByteList(value))
            b.endList()
          }
          assertRejects(
            elementsMessage(smallLimit + value.length.toLong, smallLimit, DType.UINT8)) {
            binaries.appendByteList(value)
          }
          b.endList()
          b.appendNull()
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 2L)(v.getRowCount)
            val binaryView = v.getChildColumnView(0)
            assertResult(fullRows.toLong * valuesPerRow)(binaryView.getRowCount)
            (0 until fullRows * valuesPerRow).foreach { i =>
              assert(binaryView.getBytesFromList(i).sameElements(value))
            }
            assertResult(smallLimit.toLong)(binaryView.getChildColumnView(0).getRowCount)
            assertResult(0L)(v.getEndListOffset(fullRows) - v.getStartListOffset(fullRows))
            assert(v.isNull(fullRows + 1))
          }
        }
      }
    }
  }

  test("a LIST<LIST<INT8>> column limits the rows of its inner lists") {
    val listOfByteLists = new ListType(true, new ListType(true, new BasicType(true, DType.INT8)))
    val testLimits = limits(offsetRows = smallLimit)
    val innerListsPerRow = 64
    val fullRows = smallLimit / innerListsPerRow
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(listOfByteLists, estimate)) { b =>
          val innerLists = b.getChild(0)
          (0 until fullRows).foreach { r =>
            (0 until innerListsPerRow).foreach { j =>
              innerLists.getChild(0).append((r + j).toByte)
              innerLists.endList()
            }
            b.endList()
          }
          assertRejects(rowsMessage(smallLimit + 1L, smallLimit, DType.LIST)) {
            innerLists.endList()
          }
          b.endList()
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            val innerView = v.getChildColumnView(0)
            assertResult(smallLimit.toLong)(innerView.getRowCount)
            val byteView = innerView.getChildColumnView(0)
            (0 until smallLimit).foreach { k =>
              val expected = (k / innerListsPerRow + k % innerListsPerRow).toByte
              assertResult(expected)(byteView.getByte(k))
            }
            assertResult(0L)(v.getEndListOffset(fullRows) - v.getStartListOffset(fullRows))
          }
        }
      }
    }
  }

  test("a LIST<LIST<INT8>> column limits its INT8 elements") {
    val listOfByteLists = new ListType(true, new ListType(true, new BasicType(true, DType.INT8)))
    val testLimits = limits(fixedWidthElements = smallLimit)
    val innerListsPerRow = 4
    val elementsPerInnerList = 16
    val fullRows = smallLimit / (innerListsPerRow * elementsPerInnerList)
    estimates.foreach { estimate =>
      withLimits(testLimits) {
        withResource(new RapidsHostColumnBuilder(listOfByteLists, estimate)) { b =>
          val innerLists = b.getChild(0)
          val elements = innerLists.getChild(0)
          (0 until fullRows).foreach { _ =>
            (0 until innerListsPerRow).foreach { _ =>
              (0 until elementsPerInnerList).foreach(k => elements.append(k.toByte))
              innerLists.endList()
            }
            b.endList()
          }
          assertRejects(elementsMessage(smallLimit + 1L, smallLimit, DType.INT8)) {
            elements.append(0.toByte)
          }
          b.endList()
          buildAndCheck(b, testLimits) { v =>
            assertResult(fullRows + 1L)(v.getRowCount)
            val innerView = v.getChildColumnView(0)
            assertResult(fullRows.toLong * innerListsPerRow)(innerView.getRowCount)
            val byteView = innerView.getChildColumnView(0)
            assertResult(smallLimit.toLong)(byteView.getRowCount)
            (0 until smallLimit).foreach { k =>
              assertResult((k % elementsPerInnerList).toByte)(byteView.getByte(k))
            }
          }
        }
      }
    }
  }

  test("test limits must be positive and at most the production limits") {
    val invalid = Seq(
      limits(stringBytes = 0),
      limits(stringBytes = -1),
      limits(stringBytes = production.maxStringBytes + 1),
      limits(fixedWidthElements = 0),
      limits(fixedWidthElements = production.maxFixedWidthElements + 1),
      limits(offsetRows = 0),
      limits(offsetRows = production.maxOffsetRows + 1),
      limits(structRows = 0),
      limits(structRows = production.maxStructRows + 1))
    invalid.foreach { testLimits =>
      var ran = false
      val e = intercept[IllegalArgumentException](withLimits(testLimits) { ran = true })
      assert(!e.isInstanceOf[ColumnLimitExceededException])
      assert(!ran)
    }
    // A rejected nested scope leaves the enclosing one in effect.
    withLimits(limits(stringBytes = smallLimit)) {
      intercept[IllegalArgumentException](withLimits(limits(stringBytes = 0)) { () })
      assert(!acceptsString(smallLimit + 1))
    }
    assert(acceptsString(smallLimit + 1))
    assert(withLimits(production)(acceptsString(smallLimit + 1)))
    assert(withLimits(limits(stringBytes = 1))(acceptsString(1)))
  }

  test("test limits are restored after a normal return, a thrown body and a nested scope") {
    val outer = limits(stringBytes = smallLimit)
    val inner = limits(stringBytes = smallLimit / 2)
    def outerInEffect: Boolean =
      acceptsString(smallLimit / 2 + 1) && !acceptsString(smallLimit + 1)
    val result = withLimits(outer) {
      assert(outerInEffect)
      val innerResult = withLimits(inner) {
        assert(!acceptsString(smallLimit / 2 + 1))
        "inner"
      }
      assertResult("inner")(innerResult)
      assert(outerInEffect)
      val failure = new IllegalStateException("the body failed")
      val thrown = intercept[IllegalStateException](withLimits[Unit](inner)(throw failure))
      assert(thrown eq failure)
      assert(outerInEffect)
      "outer"
    }
    assertResult("outer")(result)
    assert(acceptsString(smallLimit + 1))
    intercept[IllegalStateException] {
      withLimits[Unit](outer)(throw new IllegalStateException("the body failed"))
    }
    assert(acceptsString(smallLimit + 1))
  }

  test("a builder keeps the test limits it was created with, in its children too") {
    val arrayOfStrings = new ListType(true, stringType)
    val testLimits = limits(stringBytes = smallLimit, offsetRows = 2)
    val above = new Array[Byte](smallLimit + 1)
    withResource(new RapidsHostColumnBuilder(arrayOfStrings, 1)) { before =>
      withLimits(testLimits) {
        before.getChild(0).appendUTF8String(above)
        (0 until 3).foreach(_ => before.endList())
      }
      buildAndCheck(before, production)(v => assertResult(3L)(v.getRowCount))
    }
    val inside = withLimits(testLimits)(new RapidsHostColumnBuilder(arrayOfStrings, 1))
    withResource(inside) { b =>
      assertRejects(bytesMessage(smallLimit + 1L, smallLimit)) {
        b.getChild(0).appendUTF8String(above)
      }
      b.getChild(0).append("a")
      b.endList()
      b.endList()
      assertRejects(rowsMessage(3, 2, DType.LIST))(b.endList())
      buildAndCheck(b, testLimits)(v => assertResult(2L)(v.getRowCount))
    }
    withResource(new RapidsHostColumnBuilder(arrayOfStrings, 1)) { after =>
      after.getChild(0).appendUTF8String(above)
      (0 until 3).foreach(_ => after.endList())
      buildAndCheck(after, production)(v => assertResult(3L)(v.getRowCount))
    }
  }

  test("builders created under test limits allocate no buffer above them") {
    // Far above the limits, yet small enough that an unbounded allocation takes megabytes.
    val estimate = 1L << 20
    val testLimits = limits(smallLimit, smallLimit, smallLimit, smallLimit)
    withLimits(testLimits) {
      withResource(new RapidsHostColumnBuilder(stringType, estimate)) { b =>
        b.appendUTF8String(new Array[Byte](smallLimit / 2 + 1))
        b.appendNull()
        // Doubling the first value's buffer would allocate 2 bytes above the limit.
        b.appendUTF8String(new Array[Byte](smallLimit / 2 - 2))
        assertRejects(bytesMessage(smallLimit + 1L, smallLimit)) {
          b.appendUTF8String(new Array[Byte](2))
        }
        buildAndCheck(b, testLimits)(v => assertResult(3L)(v.getRowCount))
      }
      withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.INT64), estimate)) { b =>
        b.appendNull()
        (1 until smallLimit).foreach(i => b.append(i.toLong))
        assertRejects(elementsMessage(smallLimit + 1L, smallLimit, DType.INT64))(b.append(0L))
        buildAndCheck(b, testLimits)(v => assertResult(smallLimit.toLong)(v.getRowCount))
      }
      withResource(new RapidsHostColumnBuilder(binaryType, estimate)) { b =>
        b.appendByteList(new Array[Byte](3))
        b.appendNull()
        assertRejects(elementsMessage(smallLimit + 1L, smallLimit, DType.UINT8)) {
          b.appendByteList(new Array[Byte](smallLimit - 2))
        }
        buildAndCheck(b, testLimits)(v => assertResult(2L)(v.getRowCount))
      }
      withResource(new RapidsHostColumnBuilder(new StructType(true, intType), estimate)) { b =>
        b.getChild(0).append(1)
        b.endStruct()
        b.appendNull()
        buildAndCheck(b, testLimits)(v => assertResult(2L)(v.getRowCount))
      }
    }
  }

  test("test limits do not reach builders created on another thread") {
    withLimits(limits(stringBytes = smallLimit)) {
      var accepted: Try[Boolean] = Failure(new IllegalStateException("the thread did not run"))
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          accepted = Try(acceptsString(smallLimit + 1))
        }
      })
      thread.start()
      thread.join()
      assert(accepted.get)
      assert(!acceptsString(smallLimit + 1))
    }
  }
}
