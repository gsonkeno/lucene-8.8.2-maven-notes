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
package org.apache.lucene.util;


import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/** 
 * Class that Posting and PostingVector use to write byte
 * streams into shared fixed-size byte[] arrays.  The idea
 * is to allocate slices of increasing lengths For
 * example, the first slice is 5 bytes, the next slice is
 * 14, etc.  We start by writing our bytes into the first
 * 5 bytes.  When we hit the end of the slice, we allocate
 * the next slice and then write the address of the new
 * slice into the last 4 bytes of the previous slice (the
 * "forwarding address").
 *
 * Each slice is filled with 0's initially, and we mark
 * the end with a non-zero byte.  This way the methods
 * that are writing into the slice don't need to record
 * its length and instead allocate a new slice once they
 * hit a non-zero byte. 
 * 
 * @lucene.internal
 **/
public final class ByteBlockPool implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  public final static int BYTE_BLOCK_SHIFT = 15;
  //一个buffer(byte[])的元素个数最大为0x0001 0000
  //其对应的掩码为0x0000 FFFF
  public final static int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
  public final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte
   *  blocks. */
  public abstract static class Allocator {
    protected final int blockSize;

    public Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public void recycleByteBlocks(List<byte[]> blocks) {
      final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
      recycleByteBlocks(b, 0, b.length);
    }

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }
  
  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {
    
    public DirectAllocator() {
      this(BYTE_BLOCK_SIZE);
    }

    public DirectAllocator(int blockSize) {
      super(blockSize);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
    }
  }
  
  /** A simple {@link Allocator} that never recycles, but
   *  tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed;
    
    public DirectTrackingAllocator(Counter bytesUsed) {
      this(BYTE_BLOCK_SIZE, bytesUsed);
    }

    public DirectTrackingAllocator(int blockSize, Counter bytesUsed) {
      super(blockSize);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end-start)* blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  };

  /**
   * array of buffers currently used in the pool. Buffers are allocated if
   * needed don't modify this outside of this class.
   */
  public byte[][] buffers = new byte[10][];
  
  /** index into the buffers array pointing to the current buffer used as the head */
  private int bufferUpto = -1;                        // Which buffer we are upto
  /** Where we are in head buffer */
  //初始值为 0x0001 0000,使得第一次使用pool时，就要分配第一层级的块
  //newSlice方法执行后，值为5；把它当作指针来看的话，其总是指向块的第一个元素
  public int byteUpto = BYTE_BLOCK_SIZE;

  /** Current head buffer */
  // 当前buffer，当前类buffers二维数组中的一个buffer
  public byte[] buffer;
  /** Current head offset */
  // 当前使用中的buffer的首地址,每次调用nextBuffer()生成一个新buffer时
  // 偏移地址就增加BYTE_BLOCK_SIZE, 因为一个buffer的大小就是BYTE_BLOCK_SIZE
  // byteOffSet是BYTE_BLOCK_SIZE的整数倍
  public int byteOffset = -BYTE_BLOCK_SIZE;

  private final Allocator allocator;

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }
  
  /**
   * Resets the pool to its initial state reusing the first buffer and fills all
   * buffers with <tt>0</tt> bytes before they reused or passed to
   * {@link Allocator#recycleByteBlocks(byte[][], int, int)}. Calling
   * {@link ByteBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    reset(true, true);
  }
  
  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. Calling
   * {@link ByteBlockPool#nextBuffer()} is not needed after reset. 
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <tt>0</tt>. 
   *        This should be set to <code>true</code> if this pool is used with slices.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling
   *        {@link ByteBlockPool#nextBuffer()} is not needed after reset iff the 
   *        block pool was used before ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for(int i=0;i<bufferUpto;i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }
     
     if (bufferUpto > 0 || !reuseFirst) {
       final int offset = reuseFirst ? 1 : 0;  
       // Recycle all but the first buffer
       allocator.recycleByteBlocks(buffers, offset, 1+bufferUpto);
       Arrays.fill(buffers, offset, 1+bufferUpto, null);
     }
     if (reuseFirst) {
       // Re-use the first buffer
       bufferUpto = 0;
       byteUpto = 0;
       byteOffset = 0;
       buffer = buffers[0];
     } else {
       bufferUpto = -1;
       byteUpto = BYTE_BLOCK_SIZE;
       byteOffset = -BYTE_BLOCK_SIZE;
       buffer = null;
     }
    }
  }

  /**
   * Advances the pool to its next buffer. This method should be called once
   * after the constructor to initialize the pool. In contrast to the
   * constructor a {@link ByteBlockPool#reset()} call will advance the pool to
   * its first buffer immediately.
   */
  public void nextBuffer() {
    if (1+bufferUpto == buffers.length) {
      byte[][] newBuffers = new byte[ArrayUtil.oversize(buffers.length+1,
                                                        NUM_BYTES_OBJECT_REF)][];
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
      buffers = newBuffers;
    }
    //初始化一个buffer(即byte[])
    buffer = buffers[1+bufferUpto] = allocator.getByteBlock();
    //更新buffer指针，指向上面生成的buffer
    bufferUpto++;
    //更新byte指针，表示现在buffer的首位置等待数据写入
    byteUpto = 0;
    byteOffset += BYTE_BLOCK_SIZE;
  }
  
  /**
   * Allocates a new slice with the given size. 
   * @see ByteBlockPool#FIRST_LEVEL_SIZE
   * lucene内部调用时，size 为第一层级块的大小 5，
   */
  public int newSlice(final int size) {
    if (byteUpto > BYTE_BLOCK_SIZE-size)
      nextBuffer();
    final int upto = byteUpto;
    //第一次，byteUpto为5
    byteUpto += size;
    //第一次，buffer[4]=16
    buffer[byteUpto-1] = 16;
    return upto;
  }

  // Size of each slice.  These arrays should be at most 16
  // elements (index is encoded with 4 bits).  First array
  // is just a compact way to encode X+1 with a max.  Second
  // array is the length of each slice, ie first slice is 5
  // bytes, next slice is 14 bytes, etc.
  
  /**
   * An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY}
   * to quickly navigate to the next slice level.
   */
  public final static int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
  
  /**
   * An array holding the level sizes for byte slices.
   */
  public final static int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};
  
  /**
   * The first level size for new slices
   * @see ByteBlockPool#newSlice(int)
   */
  public final static int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

  /**
   * Creates a new byte slice with the given starting size and 
   * returns the slices offset in the pool.
   * 仅当upto已经是当前块的末尾时，此方法才被调用，分配一个新块
   * slice是一个buffer，往往容量很大，upto指向当前块的末尾
   * (upto是一个指针，从0开始计数)
   *
   * 返回值为新的块待写入新数据的指针位置(大概率还是这个buffer,除非到达边界，执行了nextBuffer方法)
   */
  public int allocSlice(final byte[] slice, final int upto) {
    //根据块的结束符slice[upto],可以推断出当前所在块的层级
    //第一层级块的结束符为16，则level=0
    //第二层级块的结束符为17，则level=1
    //......
    final int level = slice[upto] & 15;
    //得到下一层级 以及 下一层级块的大小
    final int newLevel = NEXT_LEVEL_ARRAY[level];
    final int newSize = LEVEL_SIZE_ARRAY[newLevel];

    // Maybe allocate another block
    //如果当前buffer(即byte[])已不足以分配下一层级的块，则生成下一个buffer(即byte[])
    if (byteUpto > BYTE_BLOCK_SIZE-newSize) {
      nextBuffer();
    }

    final int newUpto = byteUpto;
    final int offset = newUpto + byteOffset;
    //byteUpto指针更新，后移newSize次
    byteUpto += newSize;

    // Copy forward the past 3 bytes (which we are about
    // to overwrite with the forwarding address):
    // 当分配了新的块的时候，需要有一个指针从本块指向下一个块，使得读取此信息的时候，
    // 能够在此块读取结束后，到下一个块继续读取。

    // 这个指针需要4个byte，在本块中，除了结束符所占用的一个byte之外，
    // 之前的三个byte的数据都应该移到新的块中，从而四个byte连起来形成一个指针。
    buffer[newUpto] = slice[upto-3];
    buffer[newUpto+1] = slice[upto-2];
    buffer[newUpto+2] = slice[upto-1];

    // Write forwarding address at end of last slice:
    // 将新块地址(也即指针)写入到连同结束符在内的四个byte
    // 因为地址是int类型，需要4个字节
    slice[upto-3] = (byte) (offset >>> 24);
    slice[upto-2] = (byte) (offset >>> 16);
    slice[upto-1] = (byte) (offset >>> 8);
    slice[upto] = (byte) offset;
        
    // Write new level:
    // 在新的块的结尾写入新的结束符，结束符和层次的关系就是(endbyte = 16 | level)
    // 可能是 16|1=17; 16|2=18;....
    buffer[byteUpto-1] = (byte) (16|newLevel);

    return newUpto+3;
  }

  /** Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice.
   *  This will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses
   *  the provided {@link BytesRefBuilder} to copy bytes over. */
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
    result.length = length;

    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + length <= BYTE_BLOCK_SIZE) {
      // common case where the slice lives in a single block: just reference the buffer directly without copying
      result.bytes = buffer;
      result.offset = pos;
    } else {
      // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
      builder.grow(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  // Fill in a BytesRef from term's length & bytes encoded in
  // byte block
  // 从buffers的textStart位置起，将term的长度与内容字节全部填充到入参term中去
  public void setBytesRef(BytesRef term, int textStart) {
    final byte[] bytes = term.bytes = buffers[textStart >> BYTE_BLOCK_SHIFT];
    int pos = textStart & BYTE_BLOCK_MASK;
    if ((bytes[pos] & 0x80) == 0) {
      // length is 1 byte
      // 长度占1个字节
      // 设置term的长度以及term文本在ByteRef中的起始位置
      term.length = bytes[pos];
      term.offset = pos+1;
    } else {
      // length is 2 bytes
      // 长度占2个字节
      // 设置term的长度以及term文本在ByteRef中的起始位置
      term.length = (bytes[pos]&0x7f) + ((bytes[pos+1]&0xff)<<7);
      term.offset = pos+2;
    }
    assert term.length >= 0;
  }
  
  /**
   * Appends the bytes in the provided {@link BytesRef} at
   * the current position.
   */
  public void append(final BytesRef bytes) {
    int bytesLeft = bytes.length;
    int offset = bytes.offset;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) {
        // fits within current buffer
        System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else {
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }
  
  /**
   * Reads bytes out of the pool starting at the given offset with the given  
   * length into the given byte array at offset <tt>off</tt>.
   * <p>Note: this method allows to copy across block boundaries.</p>
   */
  public void readBytes(final long offset, final byte bytes[], int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    while (bytesLeft > 0) {
      byte[] buffer = buffers[bufferIndex++];
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Set the given {@link BytesRef} so that its content is equal to the
   * {@code ref.length} bytes starting at {@code offset}. Most of the time this
   * method will set pointers to internal data-structures. However, in case a
   * value crosses a boundary, a fresh copy will be returned.
   * On the contrary to {@link #setBytesRef(BytesRef, int)}, this does not
   * expect the length to be encoded with the data.
   */
  public void setRawBytesRef(BytesRef ref, final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + ref.length <= BYTE_BLOCK_SIZE) {
      ref.bytes = buffers[bufferIndex];
      ref.offset = pos;
    } else {
      ref.bytes = new byte[ref.length];
      ref.offset = 0;
      readBytes(offset, ref.bytes, 0, ref.length);
    }
  }

  /** Read a single byte at the given {@code offset}. */
  public byte readByte(long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    byte[] buffer = buffers[bufferIndex];
    return buffer[pos];
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.sizeOfObject(buffer);
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      if (buf == buffer) {
        continue;
      }
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }
}

