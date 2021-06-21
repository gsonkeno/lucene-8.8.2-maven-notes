package com.gson.keno.lucene;

import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BlockTreeTermsWriterTest {
    static final int OUTPUT_FLAG_HAS_TERMS = 0x2;
    static final int OUTPUT_FLAG_IS_FLOOR = 0x1;

    @Test
    public void testPendingBlockCompileIndex() throws IOException {
        // 草稿，暂用 缓存，不持久化
        RAMOutputStream scratchBytes = new RAMOutputStream();
        IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

        // 刚初始化时，指针为0，表示该位置是下个字节的写入位置，
        Assert.assertTrue(scratchBytes.getFilePointer() == 0);
        long fp = 0;
        boolean hasTerms = true;
        boolean isFloor = true;
        long output = encodeOutput(fp, hasTerms, isFloor);
        scratchBytes.writeVLong(output);

        long filePointer = scratchBytes.getFilePointer();
        System.out.println(filePointer);

        // 构建FST
        final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        final Builder<BytesRef> indexBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1,
                0, 0, true, false, Integer.MAX_VALUE,
                outputs, true, 15);
        // scratchBytes已经写了 scratchBytes.getFilePointer() 个字节了
        final byte[] bytes = new byte[(int) scratchBytes.getFilePointer()];
        // 将scratchBytes拷贝到bytes
        scratchBytes.writeTo(bytes, 0);

        // 前缀为'ace'
        final BytesRef prefix = new BytesRef(3);
        System.arraycopy(new byte[]{'a','c','e'},  0, prefix.bytes, 0,3);
        prefix.length = 3;

        // FST中添加元素，就像Map中添加key,value一样,
        // Input为IntsRef类型，所以第一个参数需要使用Util将BytesRef无符号转化为IntsRef
        // Output为Output<BytesRef>，即范型为BytesRef, 上面的ByteSequenceOutputs就是Output<BytesRef>子类
        IntsRef addInput = Util.toIntsRef(prefix, scratchIntsRef);
        BytesRef addOutput = new BytesRef(bytes, 0, bytes.length);
        indexBuilder.add( addInput, addOutput);
        // 增加的是'ace' : 3
        System.out.println("fst added " + addInput + ":" + addOutput);
        scratchBytes.reset();

        FST<BytesRef> fst = indexBuilder.finish();

        IntsRefFSTEnum<BytesRef> intsRefFSTEnum = new IntsRefFSTEnum<>(fst);
        while (intsRefFSTEnum.next() != null){
            IntsRefFSTEnum.InputOutput<BytesRef> current = intsRefFSTEnum.current();
            IntsRef input1 = current.input;
            BytesRef output1 = current.output;

            System.out.println("fst retriew " + input1.toString() + ":" + output1.toString());
        }

    }

    static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
        assert fp < (1L << 62);
        return (fp << 2) | (hasTerms ? OUTPUT_FLAG_HAS_TERMS : 0) | (isFloor ? OUTPUT_FLAG_IS_FLOOR : 0);
    }
}
