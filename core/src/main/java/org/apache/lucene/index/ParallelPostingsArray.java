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
package org.apache.lucene.index;

import org.apache.lucene.util.ArrayUtil;

/**
 * 该类的对象用来处理同一种域名的数据，有几种域名就有对应数量的ParallelPostingsArray对象，
 * 也就是说下面的数组对于不同文档的相同的域是共用的，这点很重要。
 *
 * 下面所有的数组的下标值都是termID，termID用来区分不同term的唯一标识，
 *
 * 它是一个从0开始递增的值，每个term按照处理的先后顺序获得一个termID。
 */
class ParallelPostingsArray {
  final static int BYTES_PER_POSTING = 3 * Integer.BYTES;

  final int size;
  // 每一个term在ByteBlockPool对象的buffers [ ] [ ]二维数组中的起始位置。
  final int[] textStarts; // maps term ID to the terms's text start in the bytesHash
  // 数组元素为每一个term在IntBlockPool对象的buffers[ ] [ ] 二维数组中的位置，全局指针
  final int[] addressOffset; // maps term ID to current stream address
  final int[] byteStarts; // maps term ID to stream start offset in the byte pool

  ParallelPostingsArray(final int size) {
    this.size = size;
    textStarts = new int[size];
    addressOffset = new int[size];
    byteStarts = new int[size];
  }

  int bytesPerPosting() {
    return BYTES_PER_POSTING;
  }

  ParallelPostingsArray newInstance(int size) {
    return new ParallelPostingsArray(size);
  }

  final ParallelPostingsArray grow() {
    int newSize = ArrayUtil.oversize(size + 1, bytesPerPosting());
    ParallelPostingsArray newArray = newInstance(newSize);
    copyTo(newArray, size);
    return newArray;
  }

  void copyTo(ParallelPostingsArray toArray, int numToCopy) {
    System.arraycopy(textStarts, 0, toArray.textStarts, 0, numToCopy);
    System.arraycopy(addressOffset, 0, toArray.addressOffset, 0, numToCopy);
    System.arraycopy(byteStarts, 0, toArray.byteStarts, 0, numToCopy);
  }
}
