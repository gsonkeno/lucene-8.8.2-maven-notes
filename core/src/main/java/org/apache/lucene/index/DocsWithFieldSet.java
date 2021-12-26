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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/** Accumulator for documents that have a value for a field. This is optimized
 *  for the case that all documents have a value.
 *  重点去了解 FixedBitSet, 这个类只是一个特性优化，当add的元素从0开始，且一直是连续的时候，该类的fixedBitSet为null
 *  https://www.amazingkoala.com.cn/Lucene/gongjulei/2019/0404/45.html
 *
 */
final class DocsWithFieldSet extends DocIdSet {

  private static long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocsWithFieldSet.class);

  private FixedBitSet set;
  // 表示add()过的元素总个数
  private int cost = 0;
  private int lastDocId = -1;

  /**
   * 添加的元素只能越来越大，值是docId
   * @param docID
   */
  void add(int docID) {
    if (docID <= lastDocId) {
      throw new IllegalArgumentException("Out of order doc ids: last=" + lastDocId + ", next=" + docID);
    }
    if (set != null) {
      set = FixedBitSet.ensureCapacity(set, docID);
      set.set(docID);
    } else if (docID != cost) {
      // migrate to a sparse 稀疏的 encoding using a bit set
      // 当第一次不add连续的元素时，进入到这里，比如add 0,1,2,3,4,5后，add 9
      // 在set中把0,1,2,3,4,5, 9 都加进去
      set = new FixedBitSet(docID + 1);
      set.set(0, cost);
      set.set(docID);
    }
    // 观察代码，可以发现如果第一次 add(0),则lastDocId = 0, cost = 1
    //                    第二次 add(1),则lastDocId = 1, cost = 2
    // set一直为空
    lastDocId = docID;
    cost++;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + (set == null ? 0 : set.ramBytesUsed());
  }

  @Override
  public DocIdSetIterator iterator() {
    return set != null ? new BitSetIterator(set, cost) : DocIdSetIterator.all(cost);
  }

}
