package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.tool.MyHyperLogLog;

import java.util.Arrays;

/**
 * 一个计数片，其中一个HHL 一个hash算法，一个计数表
 */
public class KeyByteFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TABLE_MAX_INDEX = InternalPageSize - 1;
    private final HashStringAlgorithm hashStringAlgorithm;

    private byte[] keyHashFragment = new byte[InternalPageSize];

    private byte[] keyHashFragmentBackUp = new byte[InternalPageSize];


    private MyHyperLogLog hyperLogLog = new MyHyperLogLog(1000);
    private MyHyperLogLog hyperLogLogBackUp = new MyHyperLogLog(1000);

    private long numberOfTimes  = 0;
    private int lostTimes  = 1;
    /**
     *
     */
    private volatile long era = 0;

    volatile boolean isAvailable = true;

    public KeyByteFragment(HashStringAlgorithm hashStringAlgorithm) {
        this.hashStringAlgorithm = hashStringAlgorithm;
    }

    /**
     * double check 进行读代级检查，不可读清除数据
     * @param key
     * @return
     */
    public boolean getAndSet(Object key){
        final int hash = hashStringAlgorithm.getHash(key);
        final int hashIndex = hash & TABLE_MAX_INDEX;
        hyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        if(times == 127){
            reBaseTable();
        }
        numberOfTimes++;
        keyHashFragment[hashIndex]++;
        int sum = get(hash);
        long groupCount = hyperLogLog.size();
        return sum > numberOfTimes / groupCount;
    }

    public long keySum(){
        return hyperLogLog.size();
    }

    public long getNumberOfTimes() {
        return numberOfTimes;
    }

    private void reBaseTable() {
        lostTimes++;
        for (int i = 0; i < keyHashFragment.length; i++){
            if(keyHashFragment[i] == 0){
                continue;
            }
            keyHashFragment[i] = (byte) (keyHashFragment[i]  >> 1);
        }

    }

    public int get(int hash){
        return keyHashFragment[hash & TABLE_MAX_INDEX] * lostTimes;
    }

    /**
     * 同步删除
     *
     */
    public long swapAndClear(long era) {
        if(this.era == era){
            return era;
        }

        boolean flag = false;
        if (!flag){
            return this.era;
        }
        if(!isAvailable){
            return this.era;
        }

        isAvailable = false;
        if(era > this.era){
            this.era = era;

            byte[] keyHashFragmentTemp = keyHashFragment;
            keyHashFragment = keyHashFragmentBackUp;
            keyHashFragmentBackUp = keyHashFragmentTemp;
            MyHyperLogLog hyperLogLogTemp = hyperLogLog.reGetMyHyperLogLog(hyperLogLog.size());
            hyperLogLog = hyperLogLogBackUp;
            hyperLogLogBackUp = hyperLogLogTemp;
            if(hyperLogLogBackUp.size() != 0){
                hyperLogLogBackUp.clear();
                Arrays.fill(keyHashFragmentBackUp, (byte) 0);
            }
        }

        this.hyperLogLog.clear();
        numberOfTimes = 0;
        lostTimes = 0;
        isAvailable = true;
        return this.era;
    }

    public boolean getHashAlgorithm(HashStringAlgorithm ha) {
        return ha.equals(hashStringAlgorithm);
    }

}
