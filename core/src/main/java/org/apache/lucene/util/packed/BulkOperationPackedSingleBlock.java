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


/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED_SINGLE_BLOCK}.
 */
final class BulkOperationPackedSingleBlock extends BulkOperation {

  private static final int BLOCK_COUNT = 1;

  private final int bitsPerValue;
  // 注意，这里的valueCount表示一个迭代批次中可以表达的源数据
  private final int valueCount;
  // bitsPerValue个Bit所能表示的最大数，也叫掩码
  private final long mask;

  public BulkOperationPackedSingleBlock(int bitsPerValue) {
    // bitsPerValue不能大于32
    assert bitsPerValue <= 32;
    this.bitsPerValue = bitsPerValue;
    this.valueCount = 64 / bitsPerValue;
    this.mask = (1L << bitsPerValue) - 1;
  }

  /**
   * 一个迭代批次中，编码源数据需要多少个 long block
   * 该类只需要一个long block
   * 即 1 long block <-----> 1 iteration
   * @return
   */
  @Override
  public final int longBlockCount() {
    return BLOCK_COUNT;
  }

  /**
   * 一个迭代批次中，编码源数据需要多少个  byte block
   * 跟longBlockCount()方法相呼应， * 8 即可
   * 即 8 byte block <----->1 iteration
   * @return
   */
  @Override
  public final int byteBlockCount() {
    return BLOCK_COUNT * 8;
  }

  /**
   * 一个迭代批次中，也即longBlockCount个 long block，可以表达多少个源数据;
   * @return
   */
  @Override
  public int longValueCount() {
    return valueCount;
  }

  /**
   * 一个迭代批次中，也即byteBlockCount个 byte block, 可以表达多少个源数据
   * @return
   */
  @Override
  public final int byteValueCount() {
    return valueCount;
  }

  /**
   * @see BulkOperation#writeLong(long, byte[], int) 可知blocks从blocksOffset位置
   * 开始取出8个byte组成一个long，先取出来的byte放在高位
   * @param blocks
   * @param blocksOffset
   * @return
   */
  private static long readLong(byte[] blocks, int blocksOffset) {
    return (blocks[blocksOffset++] & 0xFFL) << 56
        | (blocks[blocksOffset++] & 0xFFL) << 48
        | (blocks[blocksOffset++] & 0xFFL) << 40
        | (blocks[blocksOffset++] & 0xFFL) << 32
        | (blocks[blocksOffset++] & 0xFFL) << 24
        | (blocks[blocksOffset++] & 0xFFL) << 16
        | (blocks[blocksOffset++] & 0xFFL) << 8
        | blocks[blocksOffset++] & 0xFFL;
  }

  // 与下面直接相邻的方法类似，差别在于解码后的数据放到long[] 还是 int[] 中而已
  private int decode(long block, long[] values, int valuesOffset) {
    values[valuesOffset++] = block & mask;
    for (int j = 1; j < valueCount; ++j) {
      block >>>= bitsPerValue;
      values[valuesOffset++] = block & mask;
    }
    return valuesOffset;
  }

  /**
   * 从block的低位开始，每bitsPerValue个bit就组成了一个源数据
   * 为什么要从低位开算算起，而不是高位呢？因为
   * @see #encode(int[], int) 变码时写入时就是从低位开始写入的
   * @param block
   * @param values
   * @param valuesOffset
   * @return
   */
  private int decode(long block, int[] values, int valuesOffset) {
    // 这里强转为int, 有数据截断的风险，mask只要不大于Int最大值还好，所以构造函数中bitsPerValue最大也不能大于32，32就是极限了
    values[valuesOffset++] = (int) (block & mask);
    for (int j = 1; j < valueCount; ++j) {
      block >>>= bitsPerValue;
      values[valuesOffset++] = (int) (block & mask);
    }
    return valuesOffset;
  }

  // 和下面的encode方法有什么区别呢？编码源数据是long还是int的区别，其他就没有区别了
  private long encode(long[] values, int valuesOffset) {
    long block = values[valuesOffset++];
    for (int j = 1; j < valueCount; ++j) {
      block |= values[valuesOffset++] << (j * bitsPerValue);
    }
    return block;
  }

  private long encode(int[] values, int valuesOffset) {
    long block = values[valuesOffset++] & 0xFFFFFFFFL;
    // 一个迭代批次，就使用1个long block, 能编码valueCount个源数据
    // for循环中可以发现优先编码到long的低位Bit上，再逐步向高位Bit编码
    for (int j = 1; j < valueCount; ++j) {
      block |= (values[valuesOffset++] & 0xFFFFFFFFL) << (j * bitsPerValue);
    }
    return block;
  }

  // 将long[] 解码到 long[]
  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = blocks[blocksOffset++];
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  // 将byte[] 解码到 long[]
  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = readLong(blocks, blocksOffset);
      blocksOffset += 8;
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  // 将long[] 解码到 int[]
  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    for (int i = 0; i < iterations; ++i) {
      final long block = blocks[blocksOffset++];
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  // 将byte[] 解码到 int[]
  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    for (int i = 0; i < iterations; ++i) {
      final long block = readLong(blocks, blocksOffset);
      blocksOffset += 8;
      valuesOffset = decode(block, values, valuesOffset);
    }
  }

  // 将long[] 编码到long[]
  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      blocks[blocksOffset++] = encode(values, valuesOffset);
      valuesOffset += valueCount;
    }
  }

  // 将int[] 编码到 long[]
  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      blocks[blocksOffset++] = encode(values, valuesOffset);
      valuesOffset += valueCount;
    }
  }

  // 将long[] 编码到 byte[]
  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = encode(values, valuesOffset);
      valuesOffset += valueCount;
      blocksOffset = writeLong(block, blocks, blocksOffset);
    }
  }

  // 将int[] 编码到 byte[]
  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block = encode(values, valuesOffset);
      valuesOffset += valueCount;
      blocksOffset = writeLong(block, blocks, blocksOffset);
    }
  }

}
