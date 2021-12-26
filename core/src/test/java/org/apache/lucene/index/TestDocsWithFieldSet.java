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

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDocsWithFieldSet extends LuceneTestCase {

  /**
   * 稠密的;体现在add的元素是连续的
   * @throws IOException
   */
  public void testDense() throws IOException {
    DocsWithFieldSet set = new DocsWithFieldSet();
    DocIdSetIterator it = set.iterator();
    // 空集合，所以是NO_MORE_DOCS
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    set.add(0);
    it = set.iterator();
    assertEquals(0, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    long ramBytesUsed = set.ramBytesUsed();
    for (int i = 1; i < 1000; ++i) {
      set.add(i);
    }
    // 因为add的元素从0到999连续，set内部的FixedBitSet实际上并没有用到，所以ramBytesUsed不变
    assertEquals(ramBytesUsed, set.ramBytesUsed());
    it = set.iterator();
    for (int i = 0; i < 1000; ++i) {
      assertEquals(i, it.nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  /**
   * 稀疏的
   * @throws IOException
   */
  public void testSparse() throws IOException {
    DocsWithFieldSet set = new DocsWithFieldSet();
    int doc = random().nextInt(10000);
    // 添加的元素只能越来越大
    set.add(doc);
    // BitSetIterator
    DocIdSetIterator it = set.iterator();
    // 因为只有1个元素
    assertEquals(doc, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    // 注意到doc2是比doc1大的
    int doc2 = doc + TestUtil.nextInt(random(), 1, 100);
    set.add(doc2);
    // 重新获取到迭代器
    it = set.iterator();
    assertEquals(doc, it.nextDoc());
    assertEquals(doc2, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  /**
   * 前面的都是稠密的，最后一个是稀疏的
   * @throws IOException
   */
  public void testDenseThenSparse() throws IOException {
    int denseCount = random().nextInt(10000);
    int nextDoc = denseCount + random().nextInt(10000);
    DocsWithFieldSet set = new DocsWithFieldSet();
    for (int i = 0; i < denseCount; ++i) {
      set.add(i);
    }
    set.add(nextDoc);
    DocIdSetIterator it = set.iterator();
    for (int i = 0; i < denseCount; ++i) {
      assertEquals(i, it.nextDoc());
    }
    assertEquals(nextDoc, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

}
