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
package org.apache.lucene.codecs.lucene80;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene80.Lucene80NormsFormat.VERSION_CURRENT;

/**
 * Writer for {@link Lucene80NormsFormat}
 * https://www.amazingkoala.com.cn/Lucene/suoyinwenjian/2019/0305/39.html
 */
final class Lucene80NormsConsumer extends NormsConsumer {
    // .nvd,  .nvm
    IndexOutput data, meta;
    final int maxDoc;
    // dataCodec = "Lucene80NormsData"
    Lucene80NormsConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
        boolean success = false;
        try {
            // .nvd
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);

            CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            // nvd的header描述需要43个字节，filePointer表示下一个待写入的位置，写入是从0开始的
            // 所以, FilePointer既能描述已经写入了多少个字节，也能描述下一个待写入字节的位置，一语双关
            assert data.getFilePointer() == 43;

            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            maxDoc = state.segmentInfo.maxDoc();
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(-1); // write EOF marker
                CodecUtil.writeFooter(meta); // write checksum
            }
            if (data != null) {
                CodecUtil.writeFooter(data); // write checksum
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            meta = data = null;
        }
    }

    /**
     * 每个Filed只会调用一次
     * @param field field information
     * @param normsProducer NormsProducer of the numeric norm values
     * @throws IOException
     */
    @Override
    public void addNormsField(FieldInfo field, NormsProducer normsProducer) throws IOException {
        NumericDocValues values = normsProducer.getNorms(field);
        // 包含当前域的文档个数
        int numDocsWithValue = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        // 遍历包含当前field的每篇文档，
        // min为当前field在所有出现过的文档中的normValue最小值,
        // max为当前field在所有出现过的文档中的normValue最大值,
        // (每个文档中的每个field都有一个norValue值的,可见 BM25Similarity.computeNorm方法)
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            numDocsWithValue++;
            long v = values.longValue();
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        assert numDocsWithValue <= maxDoc;

        meta.writeInt(field.number);

        if (numDocsWithValue == 0) {
            assert false;
            // field没出现在任何doc中这种情况, 我想了下，实际不会出现这种情况
            meta.writeLong(-2); // docsWithFieldOffset
            meta.writeLong(0L); // docsWithFieldLength
            meta.writeShort((short) -1); // jumpTableEntryCount
            meta.writeByte((byte) -1); // denseRankPower
        } else if (numDocsWithValue == maxDoc) {
            // field出现在所有文档
            meta.writeLong(-1); // docsWithFieldOffset
            meta.writeLong(0L); // docsWithFieldLength
            meta.writeShort((short) -1); // jumpTableEntryCount
            meta.writeByte((byte) -1); // denseRankPower
        } else {
            // field只出现在部分doc中这种情况
            long offset = data.getFilePointer();
            // docsWithFieldOffset
            // nvm中第一个写nvd文件位置的指针
            meta.writeLong(offset);
            values = normsProducer.getNorms(field);
            final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            // docsWithFieldLength
            meta.writeLong(data.getFilePointer() - offset);
            meta.writeShort(jumpTableEntryCount);
            meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
        }
        // 包含当前域的文档个数
        meta.writeInt(numDocsWithValue);
        //找出当前域在所有所属文档中的最大跟最小的两个标准化值来判断存储一个标准化值最大需要的字节数，
        // 这里还是出于优化索引空间的目的。由于最小值有可能是负数，所以不能仅仅靠最大值来判断存储一个标准化值需要的字节数。
        // 比如说最小值min为 -130(需要2个字节)，最大值max为5(需要1个字节)，那么此时需要根据min来决定字节个数。
        int numBytesPerValue = numBytesPerValue(min, max);

        meta.writeByte((byte) numBytesPerValue);
        if (numBytesPerValue == 0) {
            meta.writeLong(min);
        } else {
            // normsOffset
            // nvm中第二个写nvd文件位置的指针
            meta.writeLong(data.getFilePointer());
            values = normsProducer.getNorms(field);
            writeValues(values, numBytesPerValue, data);
        }
    }

    private int numBytesPerValue(long min, long max) {
        if (min >= max) {
            return 0;
        } else if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
            return 1;
        } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
            return 2;
        } else if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE) {
            return 4;
        } else {
            return 8;
        }
    }

    private void writeValues(NumericDocValues values, int numBytesPerValue, IndexOutput out) throws IOException, AssertionError {
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            long value = values.longValue();
            switch (numBytesPerValue) {
                case 1:
                    out.writeByte((byte) value);
                    break;
                case 2:
                    out.writeShort((short) value);
                    break;
                case 4:
                    out.writeInt((int) value);
                    break;
                case 8:
                    out.writeLong(value);
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }
}
