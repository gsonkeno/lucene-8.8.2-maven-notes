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


import org.apache.lucene.store.DataOutput;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

// Packs high order byte first, to match
// IndexOutput.writeInt/Long/Short byte order

final class PackedWriter extends PackedInts.Writer {

  boolean finished;
  final PackedInts.Format format;
  final BulkOperation encoder;
  final byte[] nextBlocks;
  final long[] nextValues;
  final int iterations;
  int off;
  int written;

  PackedWriter(PackedInts.Format format, DataOutput out, int valueCount, int bitsPerValue, int mem) {
    super(out, valueCount, bitsPerValue);
    this.format = format;
    encoder = BulkOperation.of(format, bitsPerValue);
    // 这里计算iterations，是表示达到iterations个迭代批次时，进行一次flush，这样写入的效率是最高的
    iterations = encoder.computeIterations(valueCount, mem);
    // 以一次flush为基准，定义编码后需要的byte个数(  iterations * encoder.byteBlockCount()  )
    // 以及能表达的源数据个数(  iterations * encoder.byteValueCount()  )
    nextBlocks = new byte[iterations * encoder.byteBlockCount()];
    nextValues = new long[iterations * encoder.byteValueCount()];
    off = 0;
    written = 0;
    finished = false;
  }

  @Override
  protected PackedInts.Format getFormat() {
    return format;
  }

  @Override
  public void add(long v) throws IOException {
    assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
    assert !finished;
    if (valueCount != -1 && written >= valueCount) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues[off++] = v;
    // 当编码的源数据个数达到一个迭代批次能表达的源数据个数时，就要flush进入文件了
    if (off == nextValues.length) {
      flush();
    }
    // 已写入的源数据个数 + 1
    ++written;
  }

  @Override
  public void finish() throws IOException {
    assert !finished;
    if (valueCount != -1) {
      // 结束时，如果已add添加进去的源数据个数不足预先设定的valueCount个，就补0
      while (written < valueCount) {
        add(0L);
      }
    }
    flush();
    finished = true;
  }
  // 刷一次
  private void flush() throws IOException {
    encoder.encode(nextValues, 0, nextBlocks, 0, iterations);
    final int blockCount = (int) format.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue);
    out.writeBytes(nextBlocks, blockCount);
    Arrays.fill(nextValues, 0L);
    off = 0;
  }

  @Override
  public int ord() {
    return written - 1;
  }
}
