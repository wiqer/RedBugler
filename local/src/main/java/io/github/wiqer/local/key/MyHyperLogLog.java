package io.github.wiqer.local.key;

import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

public class MyHyperLogLog {
    private final HLL hyperLogLog;
    private int log2m = 4;
    public MyHyperLogLog(int nubSize) {
        if(nubSize <100){
            throw new IllegalArgumentException("nubSize must be greater than or equal to 100");
        }
        this.log2m = Math.max((getIntBitsNumber(nubSize) >>2) - 1 , 4);
        this.hyperLogLog = new HLL(log2m,5,-1, true, HLLType.EMPTY);

    }

    public MyHyperLogLog reGetMyHyperLogLog(int nubSize){
        if(Math.max((getIntBitsNumber(nubSize) >>2) - 1 , 4) != log2m){
            return new MyHyperLogLog(nubSize);
        }
        return this;
    }

    public void add(int value){
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

    private static int getIntBitsNumber(int number) {
        int bitsnumber = 0;
        number = number & Integer.MAX_VALUE;
        while (number > 0){
            bitsnumber++;
            number = number >> 1;
        }
        return bitsnumber;
    }

    public int size(){
        return (int)(hyperLogLog.cardinality() & Integer.MAX_VALUE);
    }
}
