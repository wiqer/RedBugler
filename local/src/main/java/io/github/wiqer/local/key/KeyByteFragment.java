package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashAlgorithm;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KeyByteFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TableIndexSize = InternalPageSize - 1;
    private final HashAlgorithm hashAlgorithm;

    private byte[] keyHashFragment = new byte[InternalPageSize];

    private byte[] keyHashFragmentBackUp = new byte[InternalPageSize];

    private final ReentrantLock[] keyHashFragmentLock = new ReentrantLock[InternalPageSize >> 8];

    private final ReentrantLock clearResultLock = new ReentrantLock();

    private MyHyperLogLog hyperLogLog = new MyHyperLogLog(1000);
    private MyHyperLogLog hyperLogLogBackUp = new MyHyperLogLog(1000);

    private volatile int keySum = -1;
    private long numberOfTimes  = 0;
    private int lostTimes  = 1;
    /**
     *
     */
    private volatile long era = 0;

    volatile boolean isAvailable = true;

    public KeyByteFragment(HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
        for (int i = 0; i < keyHashFragmentLock.length; i++) {
            keyHashFragmentLock[i] = new ReentrantLock();
        }
    }

    public int hash(String key, String prefix) {
        return hashAlgorithm.skipPrefixHash(key, prefix);
    }

    /**
     * 为无锁设计使用的
     * @param key
     * @param prefix
     */
    public void add(String key, String prefix){
        final int hash = hashAlgorithm.skipPrefixHash(key,prefix);
        final int hashIndex = hashAlgorithm.getHashIndex(hash, TableIndexSize);
        hyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        reBaseTable(hashIndex, times);
    }

    /**
     * double check 进行读代级检查，不可读清除数据
     * @param hash
     * @return
     */
    public void add(int hash, long era){
        if(era != swapAndClear(era)){
            return;
        }
        final int hashIndex = hashAlgorithm.getHashIndex(hash, TableIndexSize);
        hyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        ReentrantLock lock = keyHashFragmentLock[hashIndex>>8];
        lock.lock();
        try {
            reBaseTable(hashIndex, times);
        }finally {
            lock.unlock();
        }
    }

    public int keySum(){
        if(keySum != -1) return keySum;
        keySum = hyperLogLog.size();
        return keySum;
    }

    public long getNumberOfTimes() {
        return numberOfTimes;
    }

    private void reBaseTable(int hashIndex, byte times) {
        if(times == 127){
            lostTimes++;
            for (int i = 0; i < keyHashFragment.length; i++){
                if(keyHashFragment[i] == 0){
                    continue;
                }
                keyHashFragment[i] = (byte) (keyHashFragment[i]  >> 1);
            }
        }
        numberOfTimes++;
        keyHashFragment[hashIndex]++;
    }


    public int get(String key, String prefix){
         return keyHashFragment[hashAlgorithm.skipPrefixHash(key,prefix,TableIndexSize)] * lostTimes;
    }

    public int get(int hash){
        return keyHashFragment[hashAlgorithm.getHashIndex(hash,TableIndexSize)] * lostTimes;
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
        if(clearResultLock.isLocked()){
            return this.era;
        }
        try{
            flag = clearResultLock.tryLock(1, TimeUnit.MILLISECONDS);
            if (!flag){
                return this.era;
            }
            if(!isAvailable){
                return this.era;
            }

            isAvailable = false;
            keySum = -1;
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
            keySum = -1;
            isAvailable = true;

        }catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            if (flag){
                clearResultLock.unlock();
            }
        }
        return this.era;
    }

    public boolean getHashAlgorithm(HashAlgorithm ha) {
        return ha.equals(hashAlgorithm);
    }

    /**
     * 异步线程清理
     */
    public void clearBackUp(){
        boolean flag = false;
        if(clearResultLock.isLocked()){
            return;
        }
        try{
            flag = clearResultLock.tryLock(1, TimeUnit.MILLISECONDS);
            if (flag){
                if(isAvailable){
                    if(hyperLogLogBackUp.size() != 0){
                        hyperLogLogBackUp.clear();
                        Arrays.fill(keyHashFragmentBackUp, (byte) 0);
                    }
                }
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            if (flag){
                clearResultLock.unlock();
            }
        }
    }
}
