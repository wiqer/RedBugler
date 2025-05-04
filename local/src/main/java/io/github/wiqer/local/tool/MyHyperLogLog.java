package io.github.wiqer.local.tool;

import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

public class MyHyperLogLog {
    private final HLL hyperLogLog;
    private int log2m = 4;
    volatile long writeTime = -1;
    volatile long readTime = -1;
    volatile long count = 0;
    public MyHyperLogLog(int nubSize) {
        if(nubSize <100){
            throw new IllegalArgumentException("nubSize must be greater than or equal to 100");
        }
        this.log2m = (int) Math.max((getIntBitsNumber(nubSize) >>2) - 1 , 4);
        this.hyperLogLog = new HLL(log2m,5,-1, true, HLLType.EMPTY);

    }

    public MyHyperLogLog reGetMyHyperLogLog(long nubSize){
        if(Math.max((getIntBitsNumber(nubSize) >>2) - 1 , 4) != log2m){
            return new MyHyperLogLog((int) nubSize);
        }
        return this;
    }

    public void add(int value){
        writeTime = SystemClock.now();
        hyperLogLog.addRaw(value);
    }

    public void add(MyHyperLogLog other){
        hyperLogLog.union(other.getHyperLogLog());
    }

    public HLL getHyperLogLog() {
        return hyperLogLog;
    }

    public void clear(){
        hyperLogLog.clear();
    }

    private static long getIntBitsNumber(long number) {
        int bitsnumber = 0;
        number = number & Integer.MAX_VALUE;
        while (number > 0){
            bitsnumber++;
            number = number >> 1;
        }
        return bitsnumber;
    }

    public Long size(){
        if(writeTime > readTime){
            readTime = SystemClock.now();
            count = hyperLogLog.cardinality();
        }
        return count;
    }
}
