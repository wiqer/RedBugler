package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashAlgorithm;

import java.util.concurrent.locks.ReentrantLock;

public class KeyByteFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TableIndexSize = InternalPageSize - 1;
    private final HashAlgorithm hashAlgorithm;

    private final byte[] keyHashFragment = new byte[InternalPageSize];
    private final ReentrantLock[] keyHashFragmentLock = new ReentrantLock[InternalPageSize >> 8];

    MyHyperLogLog myHyperLogLog = new MyHyperLogLog(1000);
    private long numberOfTimes  = 0;
    private int lostTimes  = 1;

    public KeyByteFragment(HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
        for (int i = 0; i < keyHashFragmentLock.length; i++) {
            keyHashFragmentLock[i] = new ReentrantLock();
        }
    }

    public int hash(String key, String prefix) {
        return hashAlgorithm.skipPrefixHash(key, prefix);
    }

    public void add(String key, String prefix){
        final int hash = hashAlgorithm.skipPrefixHash(key,prefix);
        final int hashIndex = hashAlgorithm.getHashIndex(hash, TableIndexSize);
        myHyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        reBaseTable(hashIndex, times);
    }

    public void add(int hash){
        final int hashIndex = hashAlgorithm.getHashIndex(hash, TableIndexSize);
        myHyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        ReentrantLock lock = keyHashFragmentLock[hashIndex>>8];
        lock.lock();
        try {
            reBaseTable(hashIndex, times);
        }finally {
            lock.unlock();
        }

    }

    private void reBaseTable(int hashIndex, byte times) {
        if(times == 127){
            lostTimes++;
            for (int i = 0; i < keyHashFragment.length; i++){
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

    public void clear() {
        this.myHyperLogLog.clear();
        numberOfTimes = 0;
        lostTimes = 0;
    }

    public boolean getHashAlgorithm(HashAlgorithm ha) {
        return ha.equals(hashAlgorithm);
    }
}
