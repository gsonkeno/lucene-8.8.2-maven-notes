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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.util.BytesRef;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {

  private FreqProxPostingsArray freqProxPostingsArray;
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  final boolean hasFreq;
  final boolean hasProx;
  final boolean hasOffsets;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;
  TermFrequencyAttribute termFreqAtt;

  /** Set to true if any token had a payload in the current
   *  segment. */
  boolean sawPayloads;

  FreqProxTermsWriterPerField(FieldInvertState invertState, TermsHash termsHash, FieldInfo fieldInfo, TermsHashPerField nextPerField) {
    super(fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ? 2 : 1,
        termsHash.intPool, termsHash.bytePool, termsHash.termBytePool, termsHash.bytesUsed, nextPerField, fieldInfo.name, fieldInfo.getIndexOptions());
    this.fieldState = invertState;
    this.fieldInfo = fieldInfo;
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  void finish() throws IOException {
    super.finish();
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) {
    super.start(f, first);
    termFreqAtt = fieldState.termFreqAttribute;
    payloadAttribute = fieldState.payloadAttribute;
    offsetAttribute = fieldState.offsetAttribute;
    return true;
  }

  void writeProx(int termID, int proxCode) {
    if (payloadAttribute == null) {
      // 如果没有payload，位置prox信息左移1位记录下来
      writeVInt(1, proxCode<<1);
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        // 如果有payload，位置信息左移1位，且最低位记1
        // 再接着记录payload的长度，payload的信息
        writeVInt(1, (proxCode<<1)|1);
        writeVInt(1, payload.length);
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        writeVInt(1, proxCode<<1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }

  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]);
    writeVInt(1, endOffset - startOffset);
    freqProxPostingsArray.lastOffsets[termID] = startOffset;
  }

  @Override
  void newTerm(final int termID, final int docID) {
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray;

    postings.lastDocIDs[termID] = docID;
    if (!hasFreq) {
      assert postings.termFreqs == null;
      postings.lastDocCodes[termID] = docID;
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    } else {
      postings.lastDocCodes[termID] = docID << 1;
      postings.termFreqs[termID] = getTermFreq();
      if (hasProx) {
        writeProx(termID, fieldState.position);
        //IndexOptions == DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS时，hasOffsets=true
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID, final int docID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    assert !hasFreq || postings.termFreqs[termID] > 0;

    if (!hasFreq) {
      assert postings.termFreqs == null;
      if (termFreqAtt.getTermFrequency() != 1) {
        throw new IllegalStateException("field \"" + getFieldName() + "\": must index term freq while using custom TermFrequencyAttribute");
      }
      if (docID != postings.lastDocIDs[termID]) {
        // New document; now encode docCode for previous doc:
        assert docID > postings.lastDocIDs[termID];
        writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docID;
        fieldState.uniqueTermCount++;
      }
      // 表示是当前文档出现了上一篇文档内部出现的term
    } else if (docID != postings.lastDocIDs[termID]) {
      assert docID > postings.lastDocIDs[termID]:"id: "+docID + " postings ID: "+ postings.lastDocIDs[termID] + " termID: "+termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      // 由于term在上一个文档中的词频为1，故将term在上一个文档的文档号和词频组合起来存储
      if (1 == postings.termFreqs[termID]) {
        writeVInt(0, postings.lastDocCodes[termID]|1);
      } else {
        // 由于term在上一个文档中的词频>1，故将term在上一个文档的文档号和词频分开存储
        writeVInt(0, postings.lastDocCodes[termID]);
        writeVInt(0, postings.termFreqs[termID]);
      }//至此得出，对于一个doc中term而言，它归属的文档号和词频总是在处理下一个doc时，才写入到bytePool中

      // Init freq for the current document
      postings.termFreqs[termID] = getTermFreq();
      fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
      //当前文档首次出现上一文档中的某一term时，使用差值更新docCodes,且左移1位
      postings.lastDocCodes[termID] = (docID - postings.lastDocIDs[termID]) << 1;
      // term出现的最后一个文档为当前文档
      postings.lastDocIDs[termID] = docID;
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.uniqueTermCount++;
    } else {//表示当前doc当前filed 内部再次出现了已经出现过的term
      postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq());
      fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
      if (hasProx) {
        writeProx(termID, fieldState.position-postings.lastPositions[termID]);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (hasProx) {
        throw new IllegalStateException("field \"" + getFieldName() + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      //System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" + writeOffsets);
    }
    //记录每一个term在一篇文档中的词频frequencies
    int termFreqs[];                                   // # times this term occurs in the current doc
    //记录每一个term上次出现是在哪一个文档中。
    //对于同一个term来说，在某一篇文档中，只有所有该term都被处理结束才会写到倒排表中去，
    // 否则的话，term在当前文档中的词频frequencies无法正确统计。
    // 所以每次处理同一个term时，根据它目前所属的文档跟它上一次所属的文档来判断当前的操作是
    // 统计词频还是将它写入到倒排表。
    // 另外包含某个term的所有文档号是用差值存储，该数组用来计算差值。
    int lastDocIDs[];                                  // Last docID where this term occurred
    // 记录每一个term上一次出现是在哪一个文档中。跟lastDocIDs[]数组不同的是，
    // 数组元素是一个组合值，相同的是当term在新的文档中出现后，才将它上一次的文档号写入到倒排表中。
    // 基于压缩存储，如果一个term在一篇文档中的词频只有1，
    // 那么文档号跟词频的信息组合存储，否则文档号跟词频分开存储。
    int lastDocCodes[];                                // Code for prior doc
    // 记录每一个term在当前文档中上一次出现的位置。
    int lastPositions[];                               // Last position where this term occurred
    int lastOffsets[];                                 // Last endOffset where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
      if (lastPositions != null) {
        bytes += Integer.BYTES;
      }
      if (lastOffsets != null) {
        bytes += Integer.BYTES;
      }
      if (termFreqs != null) {
        bytes += Integer.BYTES;
      }

      return bytes;
    }
  }
}
