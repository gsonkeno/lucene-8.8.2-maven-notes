package com.gson.keno.lucene.ram;

import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;

public class RamTest {
    @Test
    public void test() throws InterruptedException {
        Son son = new Son();
        long shallowSizeOf = RamUsageEstimator.shallowSizeOf(son);
        System.out.println(shallowSizeOf);
        Thread.sleep(3000000000L);
    }

    public static void main(String[] args) throws InterruptedException {
        RamTest ramTest = new RamTest();
        ramTest.test();
    }

    @Test
    public void testStringShallowSize() throws InterruptedException {
        String metaM = "abcdefhsjk";
        long shallowSizeOf = RamUsageEstimator.shallowSizeOf(metaM);
        System.out.println(shallowSizeOf);
        Thread.sleep(3000000000L);
    }

    @Test
    public void testStringWrapper() throws InterruptedException {
        StringWrapper abc = new StringWrapper("abc172");
        StringWrapper abcde = new StringWrapper("defghoiquw71710aikahkahs'*1^*!&&@jhiahdsiah0918919794772");
        long shallowSizeOf = RamUsageEstimator.shallowSizeOf(abc);
        System.out.println(shallowSizeOf);

        shallowSizeOf = RamUsageEstimator.shallowSizeOf(abcde);
        System.out.println(shallowSizeOf);

        Thread.sleep(3000000000L);
    }
}
