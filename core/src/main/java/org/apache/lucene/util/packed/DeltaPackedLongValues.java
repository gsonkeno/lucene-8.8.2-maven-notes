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


import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts.Reader;

/**
 * 增量编码，编码的是相邻元素后一个对前一个的增量
 */
class DeltaPackedLongValues extends PackedLongValues {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DeltaPackedLongValues.class);

  final long[] mins;

  DeltaPackedLongValues(int pageShift, int pageMask, Reader[] values, long[] mins, long size, long ramBytesUsed) {
    super(pageShift, pageMask, values, size, ramBytesUsed);
    assert values.length == mins.length;
    this.mins = mins;
  }

  @Override
  long get(int block, int element) {
    return mins[block] + values[block].get(element);
  }

  @Override
  int decodeBlock(int block, long[] dest) {
    final int count = super.decodeBlock(block, dest);
    final long min = mins[block];
    for (int i = 0; i < count; ++i) {
      dest[i] += min;
    }
    return count;
  }

  static class Builder extends PackedLongValues.Builder {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);
    // 每次pack压缩都会产生一个min
    long[] mins;

    Builder(int pageSize, float acceptableOverheadRatio) {
      super(pageSize, acceptableOverheadRatio);
      mins = new long[values.length];
      ramBytesUsed += RamUsageEstimator.sizeOf(mins);
    }

    @Override
    long baseRamBytesUsed() {
      return BASE_RAM_BYTES_USED;
    }

    @Override
    public DeltaPackedLongValues build() {
      finish();
      pending = null;
      final PackedInts.Reader[] values = ArrayUtil.copyOfSubArray(this.values, 0, valuesOff);
      final long[] mins = ArrayUtil.copyOfSubArray(this.mins, 0, valuesOff);
      final long ramBytesUsed = DeltaPackedLongValues.BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(values) + RamUsageEstimator.sizeOf(mins);
      return new DeltaPackedLongValues(pageShift, pageMask, values, mins, size, ramBytesUsed);
    }

    /**
     * 当add的元素个数达到 {@link PackedLongValues.Builder#pending} length时，就会触发一次pack压缩
     * @param values
     * @param numValues
     * @param block
     * @param acceptableOverheadRatio
     */
    @Override
    void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
      long min = values[0];
      for (int i = 1; i < numValues; ++i) {
        min = Math.min(min, values[i]);
      }
      // values里记录的都是差量delta, 这里找出最小的delta, 将values里记录的差量delta再次编码，使差量编码所需要的bit个数再少一些,且这里的values全是正数
      for (int i = 0; i < numValues; ++i) {
        values[i] -= min;
      }
      super.pack(values, numValues, block, acceptableOverheadRatio);
      mins[block] = min;
    }

    @Override
    void grow(int newBlockCount) {
      super.grow(newBlockCount);
      ramBytesUsed -= RamUsageEstimator.sizeOf(mins);
      mins = ArrayUtil.growExact(mins, newBlockCount);
      ramBytesUsed += RamUsageEstimator.sizeOf(mins);
    }

  }

}
