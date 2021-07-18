/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.packed;



import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

/**
 * Write monotonically-increasing sequences of integers. This writer splits
 * data into blocks and then for each block, computes the average slope, the
 * minimum value and only encode the delta from the expected value using a
 * {@link DirectWriter}.
 * 
 * @see DirectMonotonicReader
 * @lucene.internal 
 */
public final class DirectMonotonicWriter {

  public static final int MIN_BLOCK_SHIFT = 2;
  public static final int MAX_BLOCK_SHIFT = 22;

  final IndexOutput meta;
  final IndexOutput data;
  final long numValues;
  final long baseDataPointer;
  final long[] buffer;
  int bufferSize;
  long count;
  boolean finished;

  DirectMonotonicWriter(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    if (blockShift < MIN_BLOCK_SHIFT || blockShift > MAX_BLOCK_SHIFT) {
      throw new IllegalArgumentException("blockShift must be in [" + MIN_BLOCK_SHIFT + "-" + MAX_BLOCK_SHIFT + "], got " + blockShift);
    }
    if (numValues < 0) {
      throw new IllegalArgumentException("numValues can't be negative, got " + numValues);
    }
    final long numBlocks = numValues == 0 ? 0 : ((numValues - 1) >>> blockShift) + 1;
    if (numBlocks > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("blockShift is too low for the provided number of values: blockShift=" + blockShift +
          ", numValues=" + numValues + ", MAX_ARRAY_LENGTH=" + ArrayUtil.MAX_ARRAY_LENGTH);
    }
    this.meta = metaOut;
    this.data = dataOut;
    this.numValues = numValues;
    final int blockSize = 1 << blockShift;
    this.buffer = new long[(int) Math.min(numValues, blockSize)];
    this.bufferSize = 0;
    this.baseDataPointer = dataOut.getFilePointer();
  }



  long previous = Long.MIN_VALUE;

  /** Write a new value. Note that data might not make it to storage until
   * {@link #finish()} is called.
   *  @throws IllegalArgumentException if values don't come in order */
  public void add(long v) throws IOException {
    if (v < previous) {
      throw new IllegalArgumentException("Values do not come in order: " + previous + ", " + v);
    }
    if (bufferSize == buffer.length) {
      flush();
    }
    buffer[bufferSize++] = v;
    previous = v;
    count++;
  }

  /** This must be called exactly once after all values have been {@link #add(long) added}. */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException("Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    if (finished) {
      throw new IllegalStateException("#finish has been called already");
    }
    if (bufferSize > 0) {
      flush();
    }
    finished = true;
  }

  private void flush() throws IOException {
    assert bufferSize != 0;
    // 假设当前的单调递增数据是 1,2,5,7, 斜率avgInc = (7-1)/3 = 2
    final float avgInc = (float) ((double) (buffer[bufferSize-1] - buffer[0]) / Math.max(1, bufferSize - 1));
    // 将原始数据1,2,5,7想象成二维坐标轴上的点,即(0,1) ,(1,2) ,(2,5) ,(3,7)
    // 另外还有一条斜率为2的穿过原点的直线,它上面有四个点(0,0), (1,2), (2,4), (3,6)
    // buffer[i]表示原始折线y值与斜率折线y值的差值, buffer[i]可正可负
    for (int i = 0; i < bufferSize; ++i) {
      final long expected = (long) (avgInc * (long) i);
      buffer[i] -= expected;
    }

    // min表示最小的差值(偏移量)
    long min = buffer[0];
    for (int i = 1; i < bufferSize; ++i) {
      min = Math.min(buffer[i], min);
    }

    long maxDelta = 0;
    // buffer[i] 表示当前差值(偏移量)与最小差值(偏移量)之前的差距
    for (int i = 0; i < bufferSize; ++i) {
      buffer[i] -= min;
      // use | will change nothing when it comes to computing required bits
      // but has the benefit of working fine with negative values too
      // (in case of overflow)
      // 记录最大差距
      maxDelta |= buffer[i];
    }

    meta.writeLong(min);
    meta.writeInt(Float.floatToIntBits(avgInc));
    meta.writeLong(data.getFilePointer() - baseDataPointer);
    if (maxDelta == 0) {
      meta.writeByte((byte) 0);
    } else {
      final int bitsRequired = DirectWriter.unsignedBitsRequired(maxDelta);
      DirectWriter writer = DirectWriter.getInstance(data, bufferSize, bitsRequired);
      for (int i = 0; i < bufferSize; ++i) {
        writer.add(buffer[i]);
      }
      writer.finish();
      meta.writeByte((byte) bitsRequired);
    }
    bufferSize = 0;
  }

  /** Returns an instance suitable for encoding {@code numValues} into monotonic
   *  blocks of 2<sup>{@code blockShift}</sup> values. Metadata will be written
   *  to {@code metaOut} and actual data to {@code dataOut}. */
  public static DirectMonotonicWriter getInstance(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    return new DirectMonotonicWriter(metaOut, dataOut, numValues, blockShift);
  }

}
