package io.github.wiqer.local.key;

import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

import java.util.BitSet;

public class MyHyperLogLog {
    private final HLL myHyperLogLog;
    private int log2m = 4;
    public MyHyperLogLog(int nubSize) {
        if(nubSize <100){
            throw new IllegalArgumentException("nubSize must be greater than or equal to 100");
        }
        this.log2m = Math.max((getIntBitsNumber(nubSize) >>2) - 1 , 4);
        this.myHyperLogLog = new HLL(log2m,5,-1, true, HLLType.EMPTY);

    }

    public void add(int value){
        myHyperLogLog.addRaw(value);
    }

    public void add(MyHyperLogLog other){
        myHyperLogLog.union(other.getMyHyperLogLog());
    }

    public HLL getMyHyperLogLog() {
        return myHyperLogLog;
    }

    public void clear(){
        myHyperLogLog.clear();
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
}
