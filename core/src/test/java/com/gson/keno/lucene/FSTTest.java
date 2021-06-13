package com.gson.keno.lucene;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;
import org.junit.Test;

import java.io.IOException;

public class FSTTest {
    @Test
    public void test() throws IOException {
        String[] inputValues = {"mo", "moth", "pop", "star", "stop", "top"};
        long[] outputValues = {100, 91, 72, 83, 54, 55};
        PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        Builder<Long> builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
        IntsRefBuilder scratchInts = new IntsRefBuilder();
        for (int i = 0; i < inputValues.length; i++) {
            //"mo"对应intsRef为[109,111]
            IntsRef intsRef = Util.toIntsRef(new BytesRef(inputValues[i]),scratchInts);
            builder.add(intsRef, outputValues[i]);
        }
        FST<Long> fst = builder.finish();
        byte[] current = new byte[39];
        FST.BytesReader reader = fst.getBytesReader();
        reader.setPosition(38);
        reader.readBytes(current, 0, current.length -1 );
        System.out.print("current数组中的值为：");
        for (int i = current.length - 1; i >= 0; i--) {
            byte b = current[i];
            System.out.print(b + " ");
        }
        System.out.println("");
        IntsRefFSTEnum<Long> intsRefFSTEnum = new IntsRefFSTEnum<>(fst);
        BytesRefBuilder builder1 = new BytesRefBuilder();
        // 顺序读
        while (intsRefFSTEnum.next() != null){
            IntsRefFSTEnum.InputOutput<Long> inputOutput = intsRefFSTEnum.current();
            BytesRef bytesRef = Util.toBytesRef(inputOutput.input, builder1);
            System.out.println(bytesRef.utf8ToString() + ":" + inputOutput.output);
        }
        // 随机读
        IntsRefFSTEnum.InputOutput<Long> longInputOutput = intsRefFSTEnum.seekExact(new IntsRef(new int[]{'s', 't', 'a', 'r'},
                0, 4));
        BytesRef bytesRef = Util.toBytesRef(longInputOutput.input, builder1);
        System.out.println("----------");
        System.out.println(bytesRef.utf8ToString() + ":" + longInputOutput.output);

    }
}
