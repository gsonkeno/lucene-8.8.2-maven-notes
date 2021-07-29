package com.gson.keno.lucene;

import org.apache.lucene.util.packed.PackedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

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

    @Test
    public void test1(){
        long value = Long.MAX_VALUE;
        // long最大值前导0的个数为1
        int i = Long.numberOfLeadingZeros(value);
        System.out.println(i);
        Assert.assertEquals(i, 1);


        // 负数前导0的个数都为0，因为首位为1
        value = Long.MIN_VALUE;
        i = Long.numberOfLeadingZeros(value);
        System.out.println(i);
        Assert.assertEquals(i, 0);

        value = -1L;
        i = Long.numberOfLeadingZeros(value);
        System.out.println(i);
        Assert.assertEquals(i, 0);
    }

    @Test
    public void test2(){
        int SUPPORTED_BITS_PER_VALUE[] = new int[] {
                1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64
        };
        int bitsRequired = 7;
        // 数组中没有7， 所以 -i - 1的值为数组中第一个大于7的元素的索引，元素是8，索引是3，所以i = -4
        int i = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired);
        System.out.println(i);
        Assert.assertEquals(i, -4);

        // 数组中有8，索引是3
        bitsRequired = 8;
        i = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired);
        System.out.println(i);
        Assert.assertEquals(i, 3);

        // 数组中所有元素都比65小，-i - 1 = 数组的长度14, 所以 i = -15
        bitsRequired = 65;
        i = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired);
        System.out.println(i);
        Assert.assertEquals(i, -15);

        // 数组中所有元素都比0大，-i - 1 = 数组的第一个元素对应的索引=0, 所以 i = -1
        bitsRequired = 0;
        i = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired);
        System.out.println(i);
        Assert.assertEquals(i, -1);
    }

    @Test
    public void test3(){
        int b = 64;
        System.out.println(b /9);
    }

    /**
     * -1表示
     */
    @Test
    public void testExpressNegative1(){
        // 1为源码, 反码 1111111.....0
        // 补码为反码
        long a = 0xFF_FF_FF_FF_FF_FF_FF_FFL;
        System.out.println(a);

        System.out.println(Long.numberOfLeadingZeros(0L));

        float f = 1.8f;
        //向下取整
        System.out.println((int)f);

        double d = 1.7d;
        // 向下取整
        System.out.println((int) d);

    }

    @Test
    public void testExpressLong(){
        long a = Long.MAX_VALUE;
        System.out.println(Long.toBinaryString(a));
        System.out.println(Long.toHexString(a));

        long b = 0x00_00_00_00_00_00_00_ff;
        // 最后一个字节全是1，即255
        System.out.println(b);
        // 强转为byte后, 11111111 表示负数， -1
        byte c = (byte)b;
        System.out.println(c);
    }


}
