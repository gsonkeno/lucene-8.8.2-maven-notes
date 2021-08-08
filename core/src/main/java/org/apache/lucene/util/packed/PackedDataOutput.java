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

import org.apache.lucene.store.DataOutput;

/**
 * A {@link DataOutput} wrapper to write unaligned, variable-length packed
 * integers.
 * 写元素，每个元素的bitsPerValue可能不一样
 * @see PackedDataInput
 * @lucene.internal
 */
public final class PackedDataOutput {

  final DataOutput out;
  long current;
  int remainingBits;

  /**
   * Create a new instance that wraps <code>out</code>.
   */
  public PackedDataOutput(DataOutput out) {
    this.out = out;
    current = 0;
    remainingBits = 8;
  }

  /**
   * Write a value using exactly <code>bitsPerValue</code> bits.
   * 将源数据value, long型按照 需要bitsPerValue个bit来表达写入到输出out中
   * long 可能比 bitsPerValue个bit所能表达的最大值小很多
   */
  public void writeLong(long value, int bitsPerValue) throws IOException {
    assert bitsPerValue == 64 || (value >= 0 && value <= PackedInts.maxValue(bitsPerValue));
    while (bitsPerValue > 0) {
      if (remainingBits == 0) {
        out.writeByte((byte) current);
        current = 0L;
        remainingBits = 8;
      }
      final int bits = Math.min(remainingBits, bitsPerValue);// 比如bitsPerValue=54,则从value的高位开始，一次8个bit写入到out中，则最后一次,bitsPerValue=6
      current = current | (((value >>> (bitsPerValue - bits)) & ((1L << bits) - 1)) << (remainingBits - bits));
      bitsPerValue -= bits;
      remainingBits -= bits;
    }
  }

  /**
   * Flush pending bits to the underlying {@link DataOutput}.
   */
  public void flush() throws IOException {
    if (remainingBits < 8) {
      out.writeByte((byte) current);
    }
    remainingBits = 8;
    current = 0L;
  }

}
