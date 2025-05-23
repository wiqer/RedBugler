package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.tool.MyHyperLogLog;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个计数片，其中一个HHL 一个hash算法，一个计数表
 */
public class KeyByteFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TABLE_MAX_INDEX = InternalPageSize - 1;
    private final HashStringAlgorithm hashStringAlgorithm;
    /**
     * 没必要上long 或者 int 本身就是模糊运算
     */
    private final byte[] keyHashFragment = new byte[InternalPageSize];

    private final MyHyperLogLog hyperLogLog = new MyHyperLogLog(1000);

    private final ReentrantLock lock = new ReentrantLock();

    private long allTimes = 0;
    private int lostTimes = 1;

    volatile boolean isAvailable = true;

    private ThreeParameterPredicate<Integer, Long, Long> predicate;

    public KeyByteFragment(HashStringAlgorithm hashStringAlgorithm, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        this.hashStringAlgorithm = hashStringAlgorithm;
    }

    public boolean getAndSet(Object key, ThreeParameterPredicate<Integer, Long, Long> pre) {
        final int hash = hashStringAlgorithm.getHash(key);

        setByHash(hash);

       final long groupCount = hyperLogLog.size();
        int sum = getSum(hash);
        return calculation(pre, sum, groupCount);
    }

    public void setByHash(int hash) {
        isAvailable = true;
        final int hashIndex = hash & TABLE_MAX_INDEX;
        hyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        if (times == 127) {
            reBaseTable();
        }
        allTimes++;
        keyHashFragment[hashIndex]++;
    }

    public void set(Object key) {

        final int hash = hashStringAlgorithm.getHash(key);
        setByHash(hash);
    }
    /**
     * double check 进行读代级检查，不可读清除数据
     *
     * @param key
     * @return
     */
    public boolean getAndSet(Object key) {
        return getAndSet(key, null);
    }

    public boolean get(Object key, ThreeParameterPredicate<Integer, Long, Long> pre) {
        if (!isAvailable) {
            return false;
        }
        final int hash = hashStringAlgorithm.getHash(key);
        int sum = getSum(hash);
        long groupCount = hyperLogLog.size();
        return calculation(pre, sum, groupCount);
    }

    private boolean calculation(ThreeParameterPredicate<Integer, Long, Long> pre, int sum, long groupCount) {
        if (groupCount <= 0) {
            return false;
        }
        if (pre != null) {
            return pre.hotKeyRule(sum, allTimes, groupCount);
        }
        if (predicate != null) {
            return predicate.hotKeyRule(sum, allTimes, groupCount);
        }
        return sum > (allTimes / groupCount)>>> 1;
    }


    /**
     * double check 进行读代级检查，不可读清除数据
     *
     * @param key
     * @return
     */
    public boolean get(Object key) {
        if (!isAvailable) {
            return false;
        }
         return get(key,null);
    }

    public long keySum() {
        return hyperLogLog.size();
    }

    public long getNumberOfTimes() {
        return allTimes;
    }

    private void reBaseTable() {
        lostTimes++;
        for (int i = 0; i < keyHashFragment.length; i++) {
            if (keyHashFragment[i] == 0) {
                continue;
            }
            keyHashFragment[i] = (byte) (keyHashFragment[i] >> 1);
        }

    }

    public int getSum(int hash) {
        return keyHashFragment[hash & TABLE_MAX_INDEX] * lostTimes;
    }

    /**
     * 同步删除
     */
    public void clear() {
        if (isAvailable) {
            boolean tryLock = lock.tryLock();
            if (tryLock) {
                try {
                    if (isAvailable) {
                        Arrays.fill(keyHashFragment, (byte) 0);
                        this.hyperLogLog.clear();
                        isAvailable = false;
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    public boolean getHashAlgorithm(HashStringAlgorithm ha) {
        return ha.equals(hashStringAlgorithm);
    }

}
