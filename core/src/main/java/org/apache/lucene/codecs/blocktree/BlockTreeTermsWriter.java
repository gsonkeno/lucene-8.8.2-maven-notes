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
package org.apache.lucene.codecs.blocktree;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

/*
  TODO:
  
    - Currently there is a one-to-one mapping of indexed
      term to term block, but we could decouple the two, ie,
      put more terms into the index than there are blocks.
      The index would take up more RAM but then it'd be able
      to avoid seeking more often and could make PK/FuzzyQ
      faster if the additional indexed terms could store
      the offset into the terms block.

    - The blocks are not written in true depth-first
      order, meaning if you just next() the file pointer will
      sometimes jump backwards.  For example, block foo* will
      be written before block f* because it finished before.
      This could possibly hurt performance if the terms dict is
      not hot, since OSs anticipate sequential file access.  We
      could fix the writer to re-order the blocks as a 2nd
      pass.

    - Each block encodes the term suffixes packed
      sequentially using a separate vInt per term, which is
      1) wasteful and 2) slow (must linear scan to find a
      particular suffix).  We should instead 1) make
      random-access array so we can directly access the Nth
      suffix, and 2) bulk-encode this array using bulk int[]
      codecs; then at search time we can binary search when
      we seek a particular term.
*/

/**
 * Block-based terms index and dictionary writer.
 * <p>
 * Writes terms dict and index, block-encoding (column
 * stride) each term's metadata for each set of terms
 * between two index terms.
 * <p>
 *
 * Files:
 * <ul>
 *   <li><tt>.tim</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 *   <li><tt>.tip</tt>: <a href="#Termindex">Term Index</a></li>
 * </ul>
 * <p>
 * <a name="Termdictionary"></a>
 * <h3>Term Dictionary</h3>
 *
 * <p>The .tim file contains the list of terms in each
 * field along with per-term statistics (such as docfreq)
 * and per-term metadata (typically pointers to the postings list
 * for that term in the inverted index).
 * </p>
 *
 * <p>The .tim is arranged in blocks: with blocks containing
 * a variable number of entries (by default 25-48), where
 * each entry is either a term or a reference to a
 * sub-block.</p>
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations:
 * the postings writer/reader are actually responsible for encoding 
 * and decoding the Postings Metadata and Term Metadata sections.</p>
 *
 * <ul>
 *    <li>TermsDict (.tim) --&gt; Header, <i>PostingsHeader</i>, NodeBlock<sup>NumBlocks</sup>,
 *                               FieldSummary, DirOffset, Footer</li>
 *    <li>NodeBlock --&gt; (OuterNode | InnerNode)</li>
 *    <li>OuterNode --&gt; EntryCount, SuffixLength, Byte<sup>SuffixLength</sup>, StatsLength, &lt; TermStats &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata</i>&gt;<sup>EntryCount</sup></li>
 *    <li>InnerNode --&gt; EntryCount, SuffixLength[,Sub?], Byte<sup>SuffixLength</sup>, StatsLength, &lt; TermStats ? &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata ? </i>&gt;<sup>EntryCount</sup></li>
 *    <li>TermStats --&gt; DocFreq, TotalTermFreq </li>
 *    <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, RootCodeLength, Byte<sup>RootCodeLength</sup>,
 *                            SumTotalTermFreq?, SumDocFreq, DocCount, LongsSize, MinTerm, MaxTerm&gt;<sup>NumFields</sup></li>
 *    <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *    <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *    <li>MinTerm,MaxTerm --&gt; {@link DataOutput#writeVInt VInt} length followed by the byte[]</li>
 *    <li>EntryCount,SuffixLength,StatsLength,DocFreq,MetaLength,NumFields,
 *        FieldNumber,RootCodeLength,DocCount,LongsSize --&gt; {@link DataOutput#writeVInt VInt}</li>
 *    <li>TotalTermFreq,NumTerms,SumTotalTermFreq,SumDocFreq --&gt; 
 *        {@link DataOutput#writeVLong VLong}</li>
 *    <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *    <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information
 *        for the BlockTree implementation.</li>
 *    <li>DirOffset is a pointer to the FieldSummary section.</li>
 *    <li>DocFreq is the count of documents which contain the term.</li>
 *    <li>TotalTermFreq is the total number of occurrences of the term. This is encoded
 *        as the difference between the total number of occurrences and the DocFreq.</li>
 *    <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)</li>
 *    <li>NumTerms is the number of unique terms for the field.</li>
 *    <li>RootCode points to the root block for the field.</li>
 *    <li>SumDocFreq is the total number of postings, the number of term-document pairs across
 *        the entire field.</li>
 *    <li>DocCount is the number of documents that have at least one posting for this field.</li>
 *    <li>LongsSize records how many long values the postings writer/reader record per term
 *        (e.g., to hold freq/prox/doc file offsets).
 *    <li>MinTerm, MaxTerm are the lowest and highest term in this field.</li>
 *    <li>PostingsHeader and TermMetadata are plugged into by the specific postings implementation:
 *        these contain arbitrary per-file data (such as parameters or versioning information) 
 *        and per-term data (such as pointers to inverted files).</li>
 *    <li>For inner nodes of the tree, every entry will steal one bit to mark whether it points
 *        to child nodes(sub-block). If so, the corresponding TermStats and TermMetaData are omitted </li>
 * </ul>
 * <a name="Termindex"></a>
 * <h3>Term Index</h3>
 * <p>The .tip file contains an index into the term dictionary, so that it can be 
 * accessed randomly.  The index is also used to determine
 * when a given term cannot exist on disk (in the .tim file), saving a disk seek.</p>
 * <ul>
 *   <li>TermsIndex (.tip) --&gt; Header, FSTIndex<sup>NumFields</sup>
 *                                &lt;IndexStartFP&gt;<sup>NumFields</sup>, DirOffset, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *   <li>IndexStartFP --&gt; {@link DataOutput#writeVLong VLong}</li>
 *   <!-- TODO: better describe FST output here -->
 *   <li>FSTIndex --&gt; {@link FST FST&lt;byte[]&gt;}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>The .tip file contains a separate FST for each
 *       field.  The FST maps a term prefix to the on-disk
 *       block that holds all terms starting with that
 *       prefix.  Each field's IndexStartFP points to its
 *       FST.</li>
 *   <li>DirOffset is a pointer to the start of the IndexStartFPs
 *       for all fields</li>
 *   <li>It's possible that an on-disk block would contain
 *       too many terms (more than the allowed maximum
 *       (default: 48)).  When this happens, the block is
 *       sub-divided into new blocks (called "floor
 *       blocks"), and then the output in the FST for the
 *       block's prefix encodes the leading byte of each
 *       sub-block, and its file pointer.
 * </ul>
 *
 * @see BlockTreeTermsReader
 * @lucene.experimental
 */
public final class BlockTreeTermsWriter extends FieldsConsumer {

  /** Suggested default value for the {@code
   *  minItemsInBlock} parameter to {@link
   *  #BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}. */
  public final static int DEFAULT_MIN_BLOCK_SIZE = 4; // 25

  /** Suggested default value for the {@code
   *  maxItemsInBlock} parameter to {@link
   *  #BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}. */
  public final static int DEFAULT_MAX_BLOCK_SIZE = 6; // 48

  //public static boolean DEBUG = false;
  //public static boolean DEBUG2 = false;

  //private final static boolean SAVE_DOT_FILES = false;

  private final IndexOutput metaOut;
  private final IndexOutput termsOut;
  private final IndexOutput indexOut;
  final int maxDoc;
  // 块内的最少元素个数
  final int minItemsInBlock;
  // 块内的最多元素个数
  final int maxItemsInBlock;

  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  final String segment;
  private static final boolean DEBUG = true;

  private final List<ByteBuffersDataOutput> fields = new ArrayList<>();

  /** Create a new writer.  The number of items (terms or
   *  sub-blocks) per block will aim to be between
   *  minItemsPerBlock and maxItemsPerBlock, though in some
   *  cases the blocks may be smaller than the min. */
  public BlockTreeTermsWriter(SegmentWriteState state,
                              PostingsWriterBase postingsWriter,
                              int minItemsInBlock,
                              int maxItemsInBlock)
    throws IOException
  {
    validateSettings(minItemsInBlock,
                     maxItemsInBlock);

    this.minItemsInBlock = minItemsInBlock;
    this.maxItemsInBlock = maxItemsInBlock;

    this.maxDoc = state.segmentInfo.maxDoc();
    this.fieldInfos = state.fieldInfos;
    this.postingsWriter = postingsWriter;
    // <segmentInfoName>_<segmentSuffix>_.<extName> 如 _0_Lucene84_0.tim
    final String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_EXTENSION);
    termsOut = state.directory.createOutput(termsName, state.context);// 生成xxxx.tim文件，但还未写数据
    boolean success = false;
    IndexOutput metaOut = null, indexOut = null;
    try {
      CodecUtil.writeIndexHeader(termsOut, BlockTreeTermsReader.TERMS_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                                 state.segmentInfo.getId(), state.segmentSuffix);

      final String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(indexName, state.context);
      CodecUtil.writeIndexHeader(indexOut, BlockTreeTermsReader.TERMS_INDEX_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                                 state.segmentInfo.getId(), state.segmentSuffix);
      segment = state.segmentInfo.name;

      final String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_META_EXTENSION);
      metaOut = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(metaOut, BlockTreeTermsReader.TERMS_META_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);

      postingsWriter.init(metaOut, state);                          // have consumer write its format/header

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut);
      }
    }
  }

  /** Throws {@code IllegalArgumentException} if any of these settings
   *  is invalid. */
  public static void validateSettings(int minItemsInBlock, int maxItemsInBlock) {
    if (minItemsInBlock <= 1) {
      throw new IllegalArgumentException("minItemsInBlock must be >= 2; got " + minItemsInBlock);
    }
    if (minItemsInBlock > maxItemsInBlock) {
      throw new IllegalArgumentException("maxItemsInBlock must be >= minItemsInBlock; got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
    }
    if (2*(minItemsInBlock-1) > maxItemsInBlock) {
      throw new IllegalArgumentException("maxItemsInBlock must be at least 2*(minItemsInBlock-1); got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {
    //if (DEBUG)
    System.out.println("\nBTTW.write seg=" + segment);

    String lastField = null;
    // 每个field单独处理
    for(String field : fields) {
      assert lastField == null || lastField.compareTo(field) < 0;
      lastField = field;

      //if (DEBUG)
      System.out.println("\nBTTW.write seg=" + segment + " field=" + field);
      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }

      TermsEnum termsEnum = terms.iterator();
      TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field));
      // 当前域是否还有未处理的term
      while (true) {
        BytesRef term = termsEnum.next();
        if (DEBUG){
          System.out.println("BTTW: next term " + term + ", utf8ToString =" + brToString(term));
        }

        if (term == null) {
          break;
        }

        if (DEBUG){
          System.out.println("--------\n" +
                  "BTTW: begin termsWriter.write field=" +
                  field + " term=" + brToString(term) );
        }
        termsWriter.write(term, termsEnum, norms);
      }

      termsWriter.finish();

      //if (DEBUG)
      System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
    }
  }
  
  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    assert fp < (1L << 62);
    return (fp << 2) | (hasTerms ? BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0) | (isFloor ? BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0);
  }

  private static class PendingEntry {
    public final boolean isTerm;

    protected PendingEntry(boolean isTerm) {
      this.isTerm = isTerm;
    }
  }

  private static final class PendingTerm extends PendingEntry {
    public final byte[] termBytes;
    // stats + metadata
    public final BlockTermState state;

    public PendingTerm(BytesRef term, BlockTermState state) {
      super(true);
      this.termBytes = new byte[term.length];
      System.arraycopy(term.bytes, term.offset, termBytes, 0, term.length);
      this.state = state;
    }

    @Override
    public String toString() {
      return "TERM: " + brToString(termBytes);
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    if (b == null) {
      return "(null)";
    } else {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(byte[] b) {
    return brToString(new BytesRef(b));
  }

  private static final class PendingBlock extends PendingEntry {
    public final BytesRef prefix;
    public final long fp;
    // 描述的是一个PendingBlock自身的信息
    public FST<BytesRef> index;
    // 描述的是该PendingBlock中嵌套的PendingBlock信息的集合（即每一个PendingBlock的index信息）
    public List<FST<BytesRef>> subIndices;
    public final boolean hasTerms;
    public final boolean isFloor;
    // 如果不是floor block生成的PendingBlock，那么该值为 -1
    public final int floorLeadByte;

    public PendingBlock(BytesRef prefix, long fp, boolean hasTerms, boolean isFloor, int floorLeadByte, List<FST<BytesRef>> subIndices) {
      super(false);
      this.prefix = prefix;
      this.fp = fp;
      this.hasTerms = hasTerms;
      this.isFloor = isFloor;
      this.floorLeadByte = floorLeadByte;
      this.subIndices = subIndices;
    }

    @Override
    public String toString() {
      return "BLOCK: prefix=" + brToString(prefix);
    }

    public void compileIndex(List<PendingBlock> blocks, RAMOutputStream scratchBytes, IntsRefBuilder scratchIntsRef) throws IOException {

      assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1): "isFloor=" + isFloor + " blocks=" + blocks;
      assert this == blocks.get(0);

      assert scratchBytes.getFilePointer() == 0;

      // TODO: try writing the leading vLong in MSB order
      // (opposite of what Lucene does today), for better
      // outputs sharing in the FST
      // 将当前block的磁盘偏移量写入scratchBytes
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      if (isFloor) {
        scratchBytes.writeVInt(blocks.size()-1);
        for (int i=1;i<blocks.size();i++) {
          PendingBlock sub = blocks.get(i);
          assert sub.floorLeadByte != -1;
          //if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
          //}
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          assert sub.fp > fp;
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }

      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final Builder<BytesRef> indexBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1,
                                                           0, 0, true, false, Integer.MAX_VALUE,
                                                           outputs, true, 15);
      //if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      //}
      //indexBuilder.DEBUG = false;
      final byte[] bytes = new byte[(int) scratchBytes.getFilePointer()];
      assert bytes.length > 0;
      scratchBytes.writeTo(bytes, 0);
      indexBuilder.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length));
      scratchBytes.reset();

      // Copy over index for all sub-blocks
      // 将 sub-block 的所有 index 写入indexBuilder
      for(PendingBlock block : blocks) {
        if (block.subIndices != null) {
          for(FST<BytesRef> subIndex : block.subIndices) {
            append(indexBuilder, subIndex, scratchIntsRef);
          }
          block.subIndices = null;
        }
      }
      // 生成新的FST
      index = indexBuilder.finish();

      assert subIndices == null;

      /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
      Util.toDot(index, w, false, false);
      System.out.println("SAVED to out.dot");
      w.close();
      */
    }

    // TODO: maybe we could add bulk-add method to
    // Builder?  Takes FST and unions it w/ current
    // FST.
    private void append(Builder<BytesRef> builder, FST<BytesRef> subIndex, IntsRefBuilder scratchIntsRef) throws IOException {
      final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
      BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
      while((indexEnt = subIndexEnum.next()) != null) {
        //if (DEBUG) {
        //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output=" + indexEnt.output);
        //}
        builder.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), indexEnt.output);
      }
    }
  }

  private final RAMOutputStream scratchBytes = new RAMOutputStream();
  private final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

  static final BytesRef EMPTY_BYTES_REF = new BytesRef();

  private static class StatsWriter {

    private final DataOutput out;
    private final boolean hasFreqs;
    private int singletonCount;

    StatsWriter(DataOutput out, boolean hasFreqs) {
      this.out = out;
      this.hasFreqs = hasFreqs;
    }

    void add(int df, long ttf) throws IOException {
      // Singletons (DF==1, TTF==1) are run-length encoded
      if (df == 1 && (hasFreqs == false || ttf == 1)) {
        singletonCount++;
      } else {
        finish(); // 看finish的注释，按照示例，这里会连续写 5, 2, 1
        out.writeVInt(df << 1);
        if (hasFreqs) {
          out.writeVLong(ttf - df);
        }
      }
    }

    void finish() throws IOException {
      if (singletonCount > 0) {
        // eg: study play foot  study
        // 当处理第二个study时, 非重term已经有singletonCount=3个
        out.writeVInt(((singletonCount - 1) << 1) | 1);
        singletonCount = 0;
      }
    }

  }

  class TermsWriter {
    private final FieldInfo fieldInfo;
    private long numTerms;
    final FixedBitSet docsSeen;
    long sumTotalTermFreq;
    long sumDocFreq;

    // Records index into pending where the current prefix at that
    // length "started"; for example, if current term starts with 't',
    // startsByPrefix[0] is the index into pending for the first
    // term/sub-block starting with 't'.  We use this to figure out when
    // to write a new block:
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();
    // eg: 依次pushTerm  abc, acc, acd, acea, aceb, acee, acef
    // 则prefixStarts = [0, 1, 3, 6]
    // 当前term为acef, 含义为terms栈中
    // 与"acef"前1字符"a"前缀开始相同的term处在terms[0] = "abc"处
    // 与"acef"前2字符"ac"前缀开始相同的term处在terms[1] = "acc"处
    // 与"acef"前3字符"ace"前缀开始相同的term处在terms[3] = "acea"处
    // 与"acef"前4字符"acef"前缀开始相同的term处在terms[6] = "acef"处
    private int[] prefixStarts = new int[8];
    // 类似栈，操作都在列表的尾部(栈顶),栈中的元素可以是term, 也可以是block
    // Pending stack of terms and blocks.  As terms arrive (in sorted order)
    // we append to this stack, and once the top of the stack has enough
    // terms starting with a common prefix, we write a new block with
    // those terms and replace those terms in the stack with a new block:
    private final List<PendingEntry> pending = new ArrayList<>();

    // Reused in writeBlocks:
    private final List<PendingBlock> newBlocks = new ArrayList<>();

    private PendingTerm firstPendingTerm;
    private PendingTerm lastPendingTerm;
    // 写入顶部的多个term到磁盘
    /** Writes the top count entries in pending, using prevTerm to compute the prefix. */
    void writeBlocks(int prefixLength, int count) throws IOException {

      assert count > 0;

      //if (DEBUG2) {
      //  BytesRef br = new BytesRef(lastTerm.bytes());
      //  br.length = prefixLength;
      //  System.out.println("writeBlocks: seg=" + segment + " prefix=" + brToString(br) + " count=" + count);
      //}

      // Root block better write all remaining pending entries:
      assert prefixLength > 0 || count == pending.size();

      int lastSuffixLeadLabel = -1;

      // True if we saw at least one term in this block (we record if a block
      // only points to sub-blocks in the terms index so we can avoid seeking
      // to it when we are looking for a term):
      // 如果这个值为true，那么这个block中至少有一个term
      boolean hasTerms = false;
      // 如果这个值为true，那么这个block中至少有一个sub-block
      boolean hasSubBlocks = false;
      // pending[start]到pending[end]拥有共同前缀
      int start = pending.size()-count;
      int end = pending.size();
      int nextBlockStart = start;
      int nextFloorLeadLabel = -1;

      for (int i=start; i<end; i++) {

        PendingEntry ent = pending.get(i);

        int suffixLeadLabel;

        if (ent.isTerm) {
          PendingTerm term = (PendingTerm) ent;
          if (term.termBytes.length == prefixLength) {
            // Suffix is 0, i.e. prefix 'foo' and term is
            // 'foo' so the term has empty string suffix
            // in this block
            // 该term如果与公共前缀相同，则suffixLeadLabel = -1
            assert lastSuffixLeadLabel == -1: "i=" + i + " lastSuffixLeadLabel=" + lastSuffixLeadLabel;
            suffixLeadLabel = -1;
          } else {
            // byte转int,正数; 记录该term与公共前缀不同的第一个字符(即后缀首字符), 这里我们叫做后缀引导字符
            suffixLeadLabel = term.termBytes[prefixLength] & 0xff;
          }
        } else {
          PendingBlock block = (PendingBlock) ent;
          assert block.prefix.length > prefixLength;
          suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;
        }
        // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" + suffixLeadLabel);

        if (suffixLeadLabel != lastSuffixLeadLabel) {
          // 计算当前位置 与 下一个block的起始位置的差值，这个值代表下一个block的Entry的个数
          int itemsInBlock = i - nextBlockStart;
          if (itemsInBlock >= minItemsInBlock && end-nextBlockStart > maxItemsInBlock) {
            // The count is too large for one block, so we must break it into "floor" blocks, where we record
            // the leading label of the suffix of the first term in each floor block, so at search time we can
            // jump to the right floor block.  We just use a naive greedy segmenter here: make a new floor
            // block as soon as we have at least minItemsInBlock.  This is not always best: it often produces
            // a too-small block as the final block:
            // 如果一个block太大，那么就划分为多个floor block，
            // 并且在每个floor block中记录后缀的第一个字符作为leading label，
            // 使得在搜索阶段能通过前缀以及leading label直接跳转到对应的floor block，
            // 另外每生成一个floor block，该block中至少包含了minItemsInBlock条PendingEntry信息，
            // 这种划分方式通常会使得最后一个block中包含的信息数量较少。
            boolean isFloor = itemsInBlock < count;
            newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, i, hasTerms, hasSubBlocks));
            // 重置
            hasTerms = false;
            hasSubBlocks = false;
            // 更新下一个block的 Lead label
            nextFloorLeadLabel = suffixLeadLabel;
            // 记录下一个block的起始位置(pending哪个位置开始)
            nextBlockStart = i;
          }

          lastSuffixLeadLabel = suffixLeadLabel;
        }

        if (ent.isTerm) {
          hasTerms = true;
        } else {
          hasSubBlocks = true;
        }
      }
      // for循环结束
      // Write last block, if any:
      if (nextBlockStart < end) {
        int itemsInBlock = end - nextBlockStart;
        boolean isFloor = itemsInBlock < count;
        newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, end, hasTerms, hasSubBlocks));
      }

      assert newBlocks.isEmpty() == false;
      // 记录写入磁盘中的所有block中的 root block
      PendingBlock firstBlock = newBlocks.get(0);

      assert firstBlock.isFloor || newBlocks.size() == 1;
      if (DEBUG){
        for (int i = 1; i < newBlocks.size(); i++) {
          if (newBlocks.get(i).subIndices != null){
            System.out.println(" writeBlocks back elements subIndices not null");
          }
        }
      }
      // 对每个写入磁盘的block的前缀 prefix构建一个FST索引
      // 所有block的FST索引联合成一个FST索引，并将联合的FST写入 root block
      firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef);

      // Remove slice from the top of the pending stack, that we just wrote:
      // 移除pending栈顶的 count个 Entry
      pending.subList(pending.size()-count, pending.size()).clear();

      // Append new block
      // 将 root block写回到 pending栈顶
      pending.add(firstBlock);
      newBlocks.clear();
    }

    private boolean allEqual(byte[] b, int startOffset, int endOffset, byte value) {
      FutureObjects.checkFromToIndex(startOffset, endOffset, b.length);
      for (int i = startOffset; i < endOffset; ++i) {
        if (b[i] != value) {
          return false;
        }
      }
      return true;
    }

    /** Writes the specified slice (start is inclusive, end is exclusive)
     *  from pending stack as a new block.  If isFloor is true, there
     *  were too many (more than maxItemsInBlock) entries sharing the
     *  same prefix, and so we broke it into multiple floor blocks where
     *  we record the starting label of the suffix of each floor block. */
    private PendingBlock writeBlock(int prefixLength, boolean isFloor, int floorLeadLabel, int start, int end,
                                    boolean hasTerms, boolean hasSubBlocks) throws IOException {

      assert end > start;
      // 记录词典文件.tim的偏移量, OutputStream缓冲区中已经有一部分数据了，这里取到下一个要写入的位置
      long startFP = termsOut.getFilePointer();

      boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;

      final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));
      System.arraycopy(lastTerm.get().bytes, 0, prefix.bytes, 0, prefixLength);
      prefix.length = prefixLength;

      //if (DEBUG2)
      System.out.println("  writeBlock field=" + fieldInfo.name + " prefix=" + brToString(prefix) + " fp=" + startFP + " isFloor=" + isFloor + " isLastInFloor=" + (end == pending.size()) + " floorLeadLabel=" + floorLeadLabel + " start=" + start + " end=" + end + " hasTerms=" + hasTerms + " hasSubBlocks=" + hasSubBlocks + " pending" + debugPending());

      // Write block header:
      // 写 block header
      // 计算block中term的数量
      int numEntries = end - start;
      int code = numEntries << 1;
      if (end == pending.size()) {
        // Last block:
        // 这里代表公共前缀为prefix的block中，这个block是最后一个block
        code |= 1;
      }
      termsOut.writeVInt(code);

      /*
      if (DEBUG) {
        System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + brToString(prefix) + " entCount=" + (end-start+1) + " startFP=" + startFP + (isFloor ? (" floorLeadLabel=" + Integer.toHexString(floorLeadLabel)) : ""));
      }
      */

      // 1st pass: pack term suffix bytes into byte[] blob
      // TODO: cutover to bulk int codec... simple64?

      // We optimize the leaf block case (block has only terms), writing a more
      // compact format in this case:
      // 这个block里面有没有sub-block
      boolean isLeafBlock = hasSubBlocks == false;

      System.out.println("  isLeaf=" + isLeafBlock);

      final List<FST<BytesRef>> subIndices;

      boolean absolute = true;

      if (isLeafBlock) {
        // Block contains only ordinary terms:
        // block 里面没有sub-block，则用压缩的更少的空间来存所有内容
        subIndices = null;
        StatsWriter statsWriter = new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i=start;i<end;i++) {
          PendingEntry ent = pending.get(i);
          assert ent.isTerm: "i=" + i;

          PendingTerm term = (PendingTerm) ent;

          assert StringHelper.startsWith(term.termBytes, prefix): "term.term=" + term.termBytes + " prefix=" + prefix;
          BlockTermState state = term.state;
          final int suffix = term.termBytes.length - prefixLength; // term长度 - 公共前缀长度 = 后缀长度
          //if (DEBUG2) {
          //  BytesRef suffixBytes = new BytesRef(suffix);
          //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
          //  suffixBytes.length = suffix;
          //  System.out.println("    write term suffix=" + brToString(suffixBytes));
          //}
          // suffixLengthsWriter 与 suffixWriter 两者配合，映射逻辑就出来了
          // For leaf block we write suffix straight
          // suffixLengthsWriter不对应真正的物理磁盘文件，只在内存中有效
          suffixLengthsWriter.writeVInt(suffix);
          // 追加[ termBytes[prefixLength], termBytes[prefixLength + suffix]), 包左不包右
          suffixWriter.append(term.termBytes, prefixLength, suffix);
          assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

          // Write term stats, to separate byte[] blob: 内存中标记，无对应磁盘文件
          statsWriter.add(state.docFreq, state.totalTermFreq);

          // Write term meta data
          // 对 term的倒排表在磁盘中的位置 写到metaWriter字节数组
          postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
          absolute = false;
        }
        statsWriter.finish();
      } else {
        // Block has at least one prefix term or a sub block:
        //  需要被写入的block 里面有sub-block
        subIndices = new ArrayList<>();
        StatsWriter statsWriter = new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i=start;i<end;i++) {
          PendingEntry ent = pending.get(i);
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;

            assert StringHelper.startsWith(term.termBytes, prefix): "term.term=" + term.termBytes + " prefix=" + prefix;
            BlockTermState state = term.state;
            final int suffix = term.termBytes.length - prefixLength;
            //if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write term suffix=" + brToString(suffixBytes));
            //}

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block, and 1 bit to record if
            // it's a prefix term.  Terms cannot be larger than ~32 KB
            // so we won't run out of bits:
            // 如果当前term不是block，后缀长度存 (suffix * 2)
            suffixLengthsWriter.writeVInt(suffix << 1);
            // 将 term的后缀写入suffixWriter字节数组
            suffixWriter.append(term.termBytes, prefixLength, suffix);
            // 将 term的 tf，df写到statsWriter字节数组
            // Write term stats, to separate byte[] blob:
            statsWriter.add(state.docFreq, state.totalTermFreq);

            // TODO: now that terms dict "sees" these longs,
            // we can explore better column-stride encodings
            // to encode all long[0]s for this block at
            // once, all long[1]s, etc., e.g. using
            // Simple64.  Alternatively, we could interleave
            // stats + meta ... no reason to have them
            // separate anymore:

            // Write term meta data
            // 对 term的倒排表在磁盘中的位置写到metaWriter字节数组
            postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
            absolute = false;
          } else {
            PendingBlock block = (PendingBlock) ent;
            assert StringHelper.startsWith(block.prefix, prefix);
            final int suffix = block.prefix.length - prefixLength;
            assert StringHelper.startsWith(block.prefix, prefix);

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block:f
            // 如果当前term不是block，后缀长度存 (suffix * 2 + 1)
            suffixLengthsWriter.writeVInt((suffix<<1)|1);
            // 将 term的后缀写入suffixWriter字节数组
            suffixWriter.append(block.prefix.bytes, prefixLength, suffix);

            //if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write sub-block suffix=" + brToString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
            //}

            assert floorLeadLabel == -1 || (block.prefix.bytes[prefixLength] & 0xff) >= floorLeadLabel: "floorLeadLabel=" + floorLeadLabel + " suffixLead=" + (block.prefix.bytes[prefixLength] & 0xff);
            assert block.fp < startFP;
            // 将sub-block在词典文件中的偏移量写到metaWriter字节数组
            suffixLengthsWriter.writeVLong(startFP - block.fp);
            // 将sub-block的term 索引加入subIndices
            subIndices.add(block.index);
            if (DEBUG){
              System.out.println("  subIndices add executed");
            }
          }
        }
        statsWriter.finish();

        assert subIndices.size() != 0;
      }
      // 选择压缩算法
      // Write suffixes byte[] blob to terms dict output, either uncompressed, compressed with LZ4 or with LowercaseAsciiCompression.
      CompressionAlgorithm compressionAlg = CompressionAlgorithm.NO_COMPRESSION;
      // If there are 2 suffix bytes or less per term, then we don't bother compressing as suffix are unlikely what
      // makes the terms dictionary large, and it also tends to be frequently the case for dense IDs like
      // auto-increment IDs, so not compressing in that case helps not hurt ID lookups by too much.
      // We also only start compressing when the prefix length is greater than 2 since blocks whose prefix length is
      // 1 or 2 always all get visited when running a fuzzy query whose max number of edits is 2.
      if (suffixWriter.length() > 2L * numEntries && prefixLength > 2) {
        // LZ4 inserts references whenever it sees duplicate strings of 4 chars or more, so only try it out if the
        // average suffix length is greater than 6.
        if (suffixWriter.length() > 6L * numEntries) {
          LZ4.compress(suffixWriter.bytes(), 0, suffixWriter.length(), spareWriter, compressionHashTable);
          if (spareWriter.getFilePointer() < suffixWriter.length() - (suffixWriter.length() >>> 2)) {
            // LZ4 saved more than 25%, go for it
            compressionAlg = CompressionAlgorithm.LZ4;
          }
        }
        if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
          spareWriter.reset();
          if (spareBytes.length < suffixWriter.length()) {
            spareBytes = new byte[ArrayUtil.oversize(suffixWriter.length(), 1)];
          }
          if (LowercaseAsciiCompression.compress(suffixWriter.bytes(), suffixWriter.length(), spareBytes, spareWriter)) {
            compressionAlg = CompressionAlgorithm.LOWERCASE_ASCII;
          }
        }
      }
      long token = ((long) suffixWriter.length()) << 3;
      if (isLeafBlock) {
        token |= 0x04;
      }
      token |= compressionAlg.code;
      // tim文件写入压缩算法code
      termsOut.writeVLong(token);
      if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
        // tim文件写入term后缀
        termsOut.writeBytes(suffixWriter.bytes(), suffixWriter.length());
      } else {
        spareWriter.writeTo(termsOut);
      }
      suffixWriter.setLength(0);
      spareWriter.reset();

      // Write suffix lengths 多个已经排序的term， 取后缀长度写到备用 spareBytes
      final int numSuffixBytes = Math.toIntExact(suffixLengthsWriter.getFilePointer());
      spareBytes = ArrayUtil.grow(spareBytes, numSuffixBytes);
      suffixLengthsWriter.writeTo(new ByteArrayDataOutput(spareBytes));
      suffixLengthsWriter.reset();
      if (allEqual(spareBytes, 1, numSuffixBytes, spareBytes[0])) {
        // Structured fields like IDs often have most values of the same length
        termsOut.writeVInt((numSuffixBytes << 1) | 1);
        termsOut.writeByte(spareBytes[0]);
      } else {
        termsOut.writeVInt(numSuffixBytes << 1);
        termsOut.writeBytes(spareBytes, numSuffixBytes);
      }

      // Stats
      final int numStatsBytes = Math.toIntExact(statsWriter.getFilePointer());
      termsOut.writeVInt(numStatsBytes);
      statsWriter.writeTo(termsOut);
      statsWriter.reset();

      // Write term meta data byte[] blob
      termsOut.writeVInt((int) metaWriter.getFilePointer());
      metaWriter.writeTo(termsOut);
      metaWriter.reset();

      // if (DEBUG) {
      //   System.out.println("      fpEnd=" + out.getFilePointer());
      // }

      if (hasFloorLeadLabel) {
        // We already allocated to length+1 above:
        prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
      }

      return new PendingBlock(prefix, startFP, hasTerms, isFloor, floorLeadLabel, subIndices);
    }

    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
      docsSeen = new FixedBitSet(maxDoc);
      postingsWriter.setField(fieldInfo);
    }

    private int[] getRealPrefixStarts(){
      int[] tmp = new int[lastTerm.length()];
      System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
      return tmp;
    }
    
    /** Writes one term's worth of postings. */
    public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {

      if (DEBUG) {
        System.out.println("BTTW: write term=" + brToString(text) +
                " prefixStarts=" + Arrays.toString(getRealPrefixStarts())
                + " pending.size()=" + pending.size());
      }

      // 将term的倒排表写入磁盘，返回磁盘偏移量
      BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);
      if (state != null) {

        assert state.docFreq != 0;
        assert fieldInfo.getIndexOptions() == IndexOptions.DOCS || state.totalTermFreq >= state.docFreq: "postingsWriter=" + postingsWriter;
        pushTerm(text);
       
        PendingTerm term = new PendingTerm(text, state);
        pending.add(term);
        if (DEBUG){
          System.out.println("BTTW: add pending term = " + term + " pending.size()=" + pending.size());
        }

        sumDocFreq += state.docFreq;
        sumTotalTermFreq += state.totalTermFreq;
        numTerms++;
        if (firstPendingTerm == null) {
          firstPendingTerm = term;
        }
        lastPendingTerm = term;
      }
    }

    /** Pushes the new term to the top of the stack, and writes new blocks. */
    private void pushTerm(BytesRef text) throws IOException {
      // Find common prefix between last term and current term:
      int prefixLength = FutureArrays.mismatch(lastTerm.bytes(), 0, lastTerm.length(), text.bytes, text.offset, text.offset + text.length);
      if (prefixLength == -1) {
        // Only happens for the first term, if it is empty
        // 第一次pushTerm才会发生，这时lastTerm为空
        assert lastTerm.length() == 0;
        prefixLength = 0;
      }
      if (DEBUG) {
        System.out.println("BTTW: shared prefixLength=" + prefixLength + "  lastTerm.length=" + lastTerm.length());
      }
      // Close the "abandoned" suffix now:
      for(int i=lastTerm.length()-1;i>=prefixLength;i--) {
        // How many items on top of the stack share the current suffix we are closing:
        int prefixTopSize = pending.size() - prefixStarts[i];
        if (DEBUG){
          System.out.println("BTTW: pushTerm write term= " + brToString(text) +", lastTerm=" + brToString(lastTerm.get())
                  + ", i=" + i + ", prefixLength=" + prefixLength + ", prefixTopSize=" + prefixTopSize
                  + ", pending.size=" + pending.size() + ", prefixStarts=" + Arrays.toString(prefixStarts)
          );
        }
        if (prefixTopSize >= minItemsInBlock) {
          if (DEBUG) {
            System.out.println("BTTW: pushTerm i=" + i + " prefixTopSize=" + prefixTopSize + " minItemsInBlock=" + minItemsInBlock);
          }
          writeBlocks(i+1, prefixTopSize);
          // why mimus (prefixTopSize-1) on prefixStarts[i] ?
          // pending[prefixStarts[i]] to pending[pending.size()-1] these term combine into one PendingBlock ,
          // and added in pending;

          // in here, you just may modify prefixStarts[prefixLength] to prefixStarts[lastTerm.length()-1] value,
          // but below "Init new tail", you will just init  prefixStarts[prefixLength] to prefixStarts[text.length-1]
          // and when you read prefixStarts[i], i always less than text.length(lastTerm.length)

          // so, what's meaning of "prefixStarts[i] -= prefixTopSize-1", may be it's unused, I guess
          prefixStarts[i] -= prefixTopSize-1;
          if (DEBUG){
            System.out.println("BTTW: pending=" + debugPending() + ",prefixStarts=" + Arrays.toString(getRealPrefixStarts()) + ",lastTerm=" + brToString(lastTerm.get()) + ",currentText=" + brToString(text));
          }
        }
      }
      // prefixStarts数组容量扩大到text.length
      if (prefixStarts.length < text.length) {
        prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
      }
      // Init new tail:
      for(int i=prefixLength;i<text.length;i++) {
        prefixStarts[i] = pending.size();
      }


      // 容易知道，此时prefixStarts的指针指向 针对的是text已经添加到pending
      lastTerm.copyBytes(text);
      if (DEBUG) {
        System.out.println("BTTW: after lastTerm.copyBytes lastTerm=" + brToString(lastTerm.get()) + ",prefixStarts=" + Arrays.toString(prefixStarts) + ",pending=" + debugPending());
      }
    }

    private String debugPending(){
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < pending.size(); i++) {
        PendingEntry pendingEntry = pending.get(i);
        sb.append(pendingEntry).append(",");
      }
      return sb.toString();
    }

    // Finishes all terms in this field
    public void finish() throws IOException {
      if (numTerms > 0) {
        // if (DEBUG) System.out.println("BTTW: finish prefixStarts=" + Arrays.toString(prefixStarts));

        // Add empty term to force closing of all final blocks:
        pushTerm(new BytesRef());

        // TODO: if pending.size() is already 1 with a non-zero prefix length
        // we can save writing a "degenerate" root block, but we have to
        // fix all the places that assume the root block's prefix is the empty string:
        pushTerm(new BytesRef());
        writeBlocks(0, pending.size());

        // We better have one final "root" block:
        assert pending.size() == 1 && !pending.get(0).isTerm: "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);
        assert root.prefix.length == 0;
        final BytesRef rootCode = root.index.getEmptyOutput();
        assert rootCode != null;

        ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        fields.add(metaOut);

        metaOut.writeVInt(fieldInfo.number);
        metaOut.writeVLong(numTerms);
        metaOut.writeVInt(rootCode.length);
        metaOut.writeBytes(rootCode.bytes, rootCode.offset, rootCode.length);
        assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
        if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
          metaOut.writeVLong(sumTotalTermFreq);
        }
        metaOut.writeVLong(sumDocFreq);
        metaOut.writeVInt(docsSeen.cardinality());
        writeBytesRef(metaOut, new BytesRef(firstPendingTerm.termBytes));
        writeBytesRef(metaOut, new BytesRef(lastPendingTerm.termBytes));
        metaOut.writeVLong(indexOut.getFilePointer());
        // Write FST to index
        root.index.save(metaOut, indexOut);
        //System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        /*
        if (DEBUG) {
          final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
          Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
          Util.toDot(root.index, w, false, false);
          System.out.println("SAVED to " + dotFileName);
          w.close();
        }
        */

      } else {
        assert sumTotalTermFreq == 0 || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
        assert sumDocFreq == 0;
        assert docsSeen.cardinality() == 0;
      }
    }

    private final RAMOutputStream suffixLengthsWriter = new RAMOutputStream();
    private final BytesRefBuilder suffixWriter = new BytesRefBuilder();
    private final RAMOutputStream statsWriter = new RAMOutputStream();
    private final RAMOutputStream metaWriter = new RAMOutputStream();
    private final RAMOutputStream spareWriter = new RAMOutputStream();
    private byte[] spareBytes = BytesRef.EMPTY_BYTES; // 备用
    private final LZ4.HighCompressionHashTable compressionHashTable = new LZ4.HighCompressionHashTable();
  }

  private boolean closed;
  
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    boolean success = false;
    try {
      metaOut.writeVInt(fields.size());
      for (ByteBuffersDataOutput fieldMeta : fields) {
        fieldMeta.copyTo(metaOut);
      }
      CodecUtil.writeFooter(indexOut);
      metaOut.writeLong(indexOut.getFilePointer());
      CodecUtil.writeFooter(termsOut);
      metaOut.writeLong(termsOut.getFilePointer());
      CodecUtil.writeFooter(metaOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut, termsOut, indexOut, postingsWriter);
      } else {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut, postingsWriter);
      }
    }
  }

  private static void writeBytesRef(DataOutput out, BytesRef bytes) throws IOException {
    out.writeVInt(bytes.length);
    out.writeBytes(bytes.bytes, bytes.offset, bytes.length);
  }
}
