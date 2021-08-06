package com.gson.keno.lucene;

public class RamUsageTest {
    private static int a;
    private static int b;
    private static int c;

    private int d;

    private int e;

    private int f;

    private Object g;

    RamUsageTest(int d, int e, int f, Object g){
        this.d = d;
        this.e = e;
        this.f = f;
        this.g = g;
    }
    public static void main(String[] args) throws InterruptedException {
        RamUsageTest ramUsageTest = new RamUsageTest(1, 2, 3, new Object());
        System.out.println(ramUsageTest);
        Thread.sleep(300000000000000L);
    }
}
