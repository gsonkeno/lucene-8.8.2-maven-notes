package com.gson.keno.lucene;

import org.apache.lucene.util.packed.PackedInts;
import org.junit.Test;

public class BitsTest {
    @Test
    public void test(){
        System.out.println(0L);
        //      00000000_00000000_00000000_00000000
        // 取反  11111111_11111111_11111111_11111111
        // 为+1取反再加1，得到，所以为-1
        System.out.println(~0L);

        // 向左移动2位，则为-1 << 2,得到-4
        // 11111111_11111111_11111111_11111100
        // 00000000_00000000_00000000_00000100 取反得到 11111111_11111111_11111111_11111011
        // 再加1得到11111111_11111111_11111111_11111100
        System.out.println(~(~0L << 2));

        System.out.println(~(~0L << 63));
        System.out.println(Long.MAX_VALUE);
        System.out.println(1<<63);
        System.out.println(PackedInts.maxValue(2));
    }
}
