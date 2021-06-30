package com.gson.keno.lucene;

import org.apache.lucene.util.FixedBitSet;
import org.junit.Assert;
import org.junit.Test;

public class FixedBitSetTest {
    @Test
    public void test1(){
        long m = 1L <<63;
        Assert.assertEquals(m , Long.MIN_VALUE);

        //左移63次一个轮回
        m = 1L << 64;
        Assert.assertEquals(m , 1L);
    }

    @Test
    public void test2(){
        FixedBitSet fixedBitSet = new FixedBitSet(300);
        fixedBitSet.set(3);
        fixedBitSet.set(17);
        fixedBitSet.set(100);
        fixedBitSet.set(192);
        fixedBitSet.set(251);
        fixedBitSet.set(299);

        Assert.assertTrue(fixedBitSet.get(3));
        Assert.assertFalse(fixedBitSet.get(4));
        Assert.assertEquals(fixedBitSet.nextSetBit(15), 17);
        Assert.assertEquals(fixedBitSet.nextSetBit(17), 17);
        Assert.assertEquals(fixedBitSet.nextSetBit(99), 100);
        Assert.assertEquals(fixedBitSet.nextSetBit(100), 100);


        Assert.assertEquals(fixedBitSet.prevSetBit(17), 17);
        Assert.assertEquals(fixedBitSet.prevSetBit(19), 17);
        Assert.assertEquals(fixedBitSet.prevSetBit(100), 100);
        Assert.assertEquals(fixedBitSet.prevSetBit(101), 100);
    }

}
