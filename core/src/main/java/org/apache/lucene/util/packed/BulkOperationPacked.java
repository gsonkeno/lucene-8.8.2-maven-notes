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
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 */
class BulkOperationPacked extends BulkOperation {

  private final int bitsPerValue;
  // 表示如果使用long型数据编码，需要多个long block; 记住, 一个 long block 8个字节，即64个bit
  private final int longBlockCount;
  // 与longBlockCount相呼应，longBlcokCount*64 个字节，每个value使用bitsPerValue个bit编码，所需要的value个数
  // 存在公式 longBlockCount * bitsPerValue = longBlockCount * 64
  private final int longValueCount;
  private final int byteBlockCount;
  private final int byteValueCount;
  private final long mask;
  private final int intMask;

  public BulkOperationPacked(int bitsPerValue) {
    this.bitsPerValue = bitsPerValue;
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    int blocks = bitsPerValue;
    // 假设一个long需要bitsPerValue=12个bit位表示
    while ((blocks & 1) == 0) {
      blocks >>>= 1;
    }
    // bitsPerValue=12时,longBlockCount = 3
    this.longBlockCount = blocks;
    // 因为bitsPerValue是longBlock的2^n次方倍数, n <=6，所以正好能够整除
    // bitsPerValue=12时,longValueCount=16;
    // 表示16个long批操作时需要的Bit数 = 16(即longValueCount) * 12(即bitsPerValue) = 192个Bit位，
    // 正好等于 3(即longBlockCount)个基本类型long(需要64个bit位)所需要的bit位 = 3 * 64 = 192个Bit位
    this.longValueCount = 64 * longBlockCount / bitsPerValue;
    int byteBlockCount = 8 * longBlockCount;
    int byteValueCount = longValueCount;
    while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) {
      byteBlockCount >>>= 1;
      byteValueCount >>>= 1;
    }
    this.byteBlockCount = byteBlockCount;
    this.byteValueCount = byteValueCount;
    if (bitsPerValue == 64) {
      this.mask = ~0L;
    } else {
      this.mask = (1L << bitsPerValue) - 1;
    }
    this.intMask = (int) mask;
    assert longValueCount * bitsPerValue == 64 * longBlockCount;
  }

  @Override
  public int longBlockCount() {
    return longBlockCount;
  }

  @Override
  public int longValueCount() {
    return longValueCount;
  }

  @Override
  public int byteBlockCount() {
    return byteBlockCount;
  }

  @Override
  public int byteValueCount() {
    return byteValueCount;
  }

  // long[] 解码到 long[]
  // 与下面相邻的方法几乎一致
  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        values[valuesOffset++] =
            ((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (blocks[blocksOffset] >>> bitsLeft) & mask;
      }
    }
  }



  // long[] 解码到 int[]
  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int bitsLeft = 64;
    // 假设bitsPerValue=31，则blocks[0]可以容纳源数据A,B,C的高2位, blocks[1]接着容纳C的剩下29位
    for (int i = 0; i < longValueCount * iterations; ++i) {
      // 解码A, bitsLeft = 64 -31=33
      // 解码B, bitsLeft = 33 -31=2
      // 解码C, bitsLeft = 2 -31=-29
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        // 解码C，这里的意识是将blocks[0]的最低2位 接续着 blocks[1]的最高29位组成31位形成源数据C写到目标int[]中
        values[valuesOffset++] = (int)
            (((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft)));
        // 解码C后，剩下-29 + 64 = 35个bit待解码
        bitsLeft += 64;
      } else {
        // 解码A, 通过&mask操作将A写到目标int[]中
        // 解码B, 通过&mask操作将A写到目标int[]中
        values[valuesOffset++] = (int) ((blocks[blocksOffset] >>> bitsLeft) & mask);
      }
    }
  }

  // byte[] 解码到 long[]
  // 与下面相邻的方法几乎一致
  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
                     int valuesOffset, int iterations) {
    long nextValue = 0L;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final long bytes = blocks[blocksOffset++] & 0xFFL;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & mask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1L << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  // byte[] 解码到 int[]
  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    // 分析代码时，重点看values数组的赋值，因为目标就是解码blocks，将解码后的源数据写到values中
    int nextValue = 0;
    int bitsLeft = bitsPerValue;
    // 假设bitsPerValue=3，blocks[0]包含A,B两个源数据，以及源数据C的2个Bit,blocks[1]包含C的剩下1个Bit
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bytes = blocks[blocksOffset++] & 0xFF;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        // 解码A时, bits = 5
        int bits = 8 - bitsLeft;
        // 解码A时, blocks[0]的高3位写到目标int[]中了
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          // 解码A后,走到这里, bits = 5-3=2
          bits -= bitsPerValue;
          // 解码B，通过&intMask，将blocks[0]的次3位继续写到目标int[]中了
          values[valuesOffset++] = (bytes >>> bits) & intMask;
        }
        // then buffer
        // 解码B后, bits =2, bitsLeft = 3-2=1
        bitsLeft = bitsPerValue - bits;
        // 这里相当于将blocks[0]的最低2位，向高位左移1次，目的是为了腾出最低位让下一个block，即blocks[1]的最高位填充进去
        // 以此形成3个bit构成源数据C，再循环一次进入else分支，你就看明白了
        nextValue = (bytes & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  // long[] 编码到 long[]
  // 与下面直接相邻的方法实现几乎相同
  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        nextBlock |= values[valuesOffset++] << bitsLeft;
      } else if (bitsLeft == 0) {
        nextBlock |= values[valuesOffset++];
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        nextBlock |= values[valuesOffset] >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        bitsLeft += 64;
      }
    }
  }

  // int[] 编码到 long[]
  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    // 举个例子, bitsPerValue=31,现在有三个源数据A,B,C要编码
    for (int i = 0; i < longValueCount * iterations; ++i) {
      // A编码后, bitsLeft=33
      // B编码后, bitsLeft=2
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        // A编码到nextBlock的高31位
        // B编码到nextBlock的次高31位
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << bitsLeft;
      } else if (bitsLeft == 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL);
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        // C编码时,bitsLeft=2-31=-29，当前block只能存2个bit了，剩下29个bit要写到下一个block中了
        // 这里保留C的最高2位写到当前block剩余的2个bit中
        nextBlock |= (values[valuesOffset] & 0xFFFFFFFFL) >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        // 下一个block写入C的低29位，并将数据写到block的高位上
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        // C编码后，当前block只能-29+64=35个bit位可用了
        bitsLeft += 64;
      }
    }
  }

  // 编码long[] 到 byte[]
  // 跟下面相邻的方法几乎一样
  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final long v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft;
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits;
        nextBlock = (int) ((v & ((1L << bits) - 1)) << bitsLeft);
      }
    }
    assert bitsLeft == 8;
  }

  // 编码int[] 到 byte[]
  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    // nextBlock起到一个中转站的作用，存的数据一直都没超过8个bit
    int nextBlock = 0;
    int bitsLeft = 8;
    // 这里举个例子，有4个源数据 001(1) A, 010(2) B, 011(3) C, 110(6) D 要进行编码
    // bitsPerValue = 3,
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int v = values[valuesOffset++];
      assert PackedInts.bitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        // A编码后, nextBlock 为 001_00000，将A编码到nextBlock的高位了
        // B编码后, nextBlock 为 001010_00, 将B编码到nextBlock的次高位了
        // C编码时,bitsLeft为2，走到下面的else分支了
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        // 编码C时，bitsLeft=2，则bits=1,表示1个bit要到编码到下一个block,当前block只能编码2个bit了
        int bits = bitsPerValue - bitsLeft;
        // 将C的高2位11留下来，继续编码到当前block中
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        // 编码C时，将剩余的1个bit继续在下一个block中进行编码
        bitsLeft = 8 - bits;
        // v & ((1 << bits) - 1)表示v中剩余的未编码的bit位，编码C时，剩下1位
        // 左移bitsLeft=7次，表示编码到下一个block的高位上
        nextBlock = (v & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == 8;
  }

}
