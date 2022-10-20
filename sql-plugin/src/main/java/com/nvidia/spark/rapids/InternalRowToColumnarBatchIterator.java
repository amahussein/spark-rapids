/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.NvtxColor;
import ai.rapids.cudf.NvtxRange;
import ai.rapids.cudf.Table;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Option;
import scala.collection.Iterator;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * This class converts InternalRow instances to ColumnarBatches on the GPU through the magic of
 * code generation. This just provides most of the framework a concrete implementation will
 * be generated based off of the schema.
 * The InternalRow instances are first converted to UnsafeRow, cheaply if the instance is already
 * UnsafeRow, and then the UnsafeRow data is collected into a ColumnarBatch.
 */
public abstract class InternalRowToColumnarBatchIterator implements Iterator<ColumnarBatch> {
  protected final Iterator<InternalRow> input;
  protected UnsafeRow pending = null;
  // The initial row size estimate. The value is calculated based on column types. It represents the
  // minimum size needed to be reserved for a row (including ones with variable width columns).
  protected final int rowSizeEstimate;
  // Caches the actual row size used successfully to allocate at least one row. This variable
  // is always EQ to rowSizeEstimate unless the dataBuffer cannot fit a single row. In the latter
  // case, it will be GE rowSizeEstimate.
  // Note that: while this variable represents the adjusted bytes needed per row, still the
  //            rowSizeEstimate value is used to check if the remaining bytes can fit a new row.
  protected int adjustedRowSize;
  // The number of rows per batch. It is driven by adjustedRowSize.
  protected int adjustedNumRows;
  // The size of the data buffer in bytes. It is recalculated everytime the adjustedRowSize is updated.
  protected long dataLength;
  protected final long goalTargetSize;
  protected final DType[] rapidsTypes;
  protected final DataType[] outputTypes;
  protected final GpuMetric semaphoreWaitTime;
  protected final GpuMetric streamTime;
  protected final GpuMetric opTime;
  protected final GpuMetric numInputRows;
  protected final GpuMetric numOutputRows;
  protected final GpuMetric numOutputBatches;
  // Flag to indicate whether the schema fits CUDF kernel optimization.
  // Note that this flag is driven by the initial estimated row size.
  protected final boolean fitsOptimizedConversion;

  protected InternalRowToColumnarBatchIterator(
      Iterator<InternalRow> input,
      Attribute[] schema,
      CoalesceSizeGoal goal,
      GpuMetric semaphoreWaitTime,
      GpuMetric streamTime,
      GpuMetric opTime,
      GpuMetric numInputRows,
      GpuMetric numOutputRows,
      GpuMetric numOutputBatches) {
    this.input = input;
    JCudfUtil.RowOffsetsCalculator cudfRowEstimator = JCudfUtil.getRowOffsetsCalculator(schema);
    rowSizeEstimate = cudfRowEstimator.getEstimateSize();
    // Check if the row fits the CUDF kernel optimization.
    fitsOptimizedConversion = JCudfUtil.fitsOptimizedConversion(cudfRowEstimator);
    goalTargetSize = goal.targetSizeBytes();
    // Set adjustedRowSize, adjustedNumRows and dataLength fields based on the initial estimate.
    setActualDataCapacities(rowSizeEstimate);
    rapidsTypes = new DType[schema.length];
    outputTypes = new DataType[schema.length];

    for (int i = 0; i < schema.length; i++) {
      rapidsTypes[i] = GpuColumnVector.getNonNestedRapidsType(schema[i].dataType());
      outputTypes[i] = schema[i].dataType();
    }
    this.semaphoreWaitTime = semaphoreWaitTime;
    this.streamTime = streamTime;
    this.opTime = opTime;
    this.numInputRows = numInputRows;
    this.numOutputRows = numOutputRows;
    this.numOutputBatches = numOutputBatches;
  }

  @Override
  public boolean hasNext() {
    boolean ret = true;
    if (pending == null) {
      long start = System.nanoTime();
      ret = input.hasNext();
      long ct = System.nanoTime() - start;
      streamTime.add(ct);
    }
    return ret;
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final int BYTES_PER_OFFSET = DType.INT32.getSizeInBytes();

    long collectStart = System.nanoTime();

    ColumnVector devColumn = null;
    NvtxRange buildRange = null;
    // The row formatted data is stored as a column of lists of bytes.  The current java CUDF APIs
    // don't do a great job from a performance standpoint with building this type of data structure,
    // and we want this to be as efficient as possible, so we are going to allocate two host memory
    // buffers.  One will be for the byte data and the second will be for the offsets. We will then
    // write the data directly into those buffers using code generation in a child of this class.
    // that implements fillBatch.
    boolean fillBatchDone = false;
    int originalRowSize = adjustedRowSize;
    int retryCount = 0;
    while (!fillBatchDone) { // try until we find the correct size to allocate the dataBuffer
      try (HostMemoryBuffer dataBuffer = HostMemoryBuffer.allocate(dataLength);
           HostMemoryBuffer offsetsBuffer =
               HostMemoryBuffer.allocate(((long) adjustedNumRows + 1) * BYTES_PER_OFFSET)) {

        int[] used = fillBatch(dataBuffer, offsetsBuffer);
        int dataOffset = used[0];
        int currentRow = used[1];

        if (currentRow == 0) {
          // if we fail to copy at least one row, it means that the allocated buffer cannot fit
          // the current row. Throw and exception to increase the allocated buffer.
          throw new BufferOverflowException();
        }
        fillBatchDone = true;

        if (numInputRows != null) {
          numInputRows.add(currentRow);
        }
        if (numOutputRows != null) {
          numOutputRows.add(currentRow);
        }
        if (numOutputBatches != null) {
          numOutputBatches.add(1);
        }
        // Now that we have filled the buffers with the data, we need to turn them into a
        // HostColumnVector and copy them to the device so the GPU can turn it into a Table.
        // To do this we first need to make a HostColumnCoreVector for the data, and then
        // put that into a HostColumnVector as its child.  This the basics of building up
        // a column of lists of bytes in CUDF, but it is typically hidden behind the higher level
        // APIs.
        dataBuffer.incRefCount();
        offsetsBuffer.incRefCount();
        try (HostColumnVectorCore dataCv =
                 new HostColumnVectorCore(DType.INT8, dataOffset, Optional.of(0L),
                     dataBuffer, null, null, new ArrayList<>());
             HostColumnVector hostColumn = new HostColumnVector(DType.LIST,
                 currentRow, Optional.of(0L), null, null,
                 offsetsBuffer, Collections.singletonList(dataCv))) {

          long ct = System.nanoTime() - collectStart;
          streamTime.add(ct);

          // Grab the semaphore because we are about to put data onto the GPU.
          TaskContext tc = TaskContext.get();
          if (tc != null) {
            GpuSemaphore$.MODULE$.acquireIfNecessary(tc, semaphoreWaitTime);
          }
          buildRange = NvtxWithMetrics.apply("RowToColumnar: build", NvtxColor.GREEN, Option.apply(opTime));
          devColumn = hostColumn.copyToDevice();
        }
        // Re-evaluate the allocated memory buffer if necessary for the upcoming next() calls.
        if (retryCount > 0) {
          // Only happens for variable sized columns.
          // This means that the estimated row size was smaller than what we need to
          // copy a single object.
          // There are several options listed below (current algo implements 2nd option):
          // 1- Continue without updating the initial dataLength considering that
          //    this batch is an outlier. This approach saves memory.
          // 2- Set the rowSize to the highest value from the current iteration.
          // 3- The middle-way approach to keep balance between memory and performance.
          //    To avoid outliers, update the estimated row size using median between two values
          //    which will be used for the upcoming calls. This way, finding the minimum dataBuffer
          //    can be found faster until eventually steady state is reached.

          // First, take average window to omit outlier.
          long averageRowSize = dataOffset / currentRow;
          // Second, get the median between the average window size and the cached size prior to allocating
          // that batch.
          long currMedian = (averageRowSize + originalRowSize) / 2;
          // Increase the allocated memory by setting the adjustedRowSize to the median between the
          // current estimate and the latest size that successfully copied the row.
          setActualDataCapacities(currMedian);
        }
      } catch (BufferOverflowException ex) {
        // Handle corner case when the dataLength is too small to copy a single row.
        // In this corner case, the entire dataBuffer does not fit a single row. So, grow the rowSize
        // to 2x the current dataLength.
        retryCount++;
        if (dataLength >= JCudfUtil.JCUDF_MAX_ROW_BUFFER_LENGTH) {
          // Row size cannot exceed 2^31. Proceed with throwing exception.
          throw new RuntimeException(
              "JCudf row is too large to fit in MemoryBuffer. Retries count = " + retryCount, ex);
        }
        // Recalculate the buffer size after setting the rowSize to 2 * dataLength.
        setActualDataCapacities(dataLength << 1);
      }
    }
    try (NvtxRange ignored = buildRange;
         ColumnVector cv = devColumn;
         Table tab = fitsOptimizedConversion ?
             // The fixed-width optimized cudf kernel only supports up to 1.5 KB per row which
             // means at most 184 double/long values.
             Table.convertFromRowsFixedWidthOptimized(cv, rapidsTypes) :
             Table.convertFromRows(cv, rapidsTypes)) {
      return GpuColumnVector.from(tab, outputTypes);
    }
  }

  protected void setActualDataCapacities(final long suggestedRowSize) {
    // Make sure that we never fall below the initial row size estimate.
    int newRowSize = (int) Math.min(Math.max(rowSizeEstimate, suggestedRowSize), JCudfUtil.JCUDF_MAX_ROW_BUFFER_LENGTH);
    adjustedRowSize = JCudfUtil.alignOffset(newRowSize, JCudfUtil.JCUDF_ROW_ALIGNMENT);
    adjustedNumRows = (int) Math.max(1, Math.min(Integer.MAX_VALUE - 1, goalTargetSize / adjustedRowSize));
    dataLength = (long) adjustedRowSize * adjustedNumRows;
  }
  /**
   * Fill a batch with data.  This is the abstraction point because it is faster to have a single
   * virtual function call per batch instead of one per row.
   * @param dataBuffer the data buffer to populate
   * @param offsetsBuffer the offsets buffer to populate
   * @return an array of ints where the first index is the amount of data in bytes copied into
   * the data buffer and the second index is the number of rows copied into the buffers.
   */
  public abstract int[] fillBatch(
      HostMemoryBuffer dataBuffer,
      HostMemoryBuffer offsetsBuffer);
}
