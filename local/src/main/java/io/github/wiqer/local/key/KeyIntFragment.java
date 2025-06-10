package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.tool.MyHyperLogLog;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个计数片，使用AtomicIntegerArray存储数据，提供并发安全的计数功能
 */
public class KeyIntFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TABLE_MAX_INDEX = InternalPageSize - 1;
    private final HashStringAlgorithm hashStringAlgorithm;
    
    /**
     * 使用AtomicIntegerArray存储，提供并发安全的计数功能
     * 相比int[]的优势：
     * 1. 内置并发控制
     * 2. 原子操作支持
     * 3. 避免伪共享
     */
    private final AtomicIntegerArray keyHashFragment = new AtomicIntegerArray(InternalPageSize);

    private final MyHyperLogLog hyperLogLog = new MyHyperLogLog(100000);

    private final ReentrantLock lock = new ReentrantLock();
    private final ReentrantLock rebalanceLock = new ReentrantLock();

    private final AtomicLong allTimes = new AtomicLong(0);
    private final AtomicLong lostTimes = new AtomicLong(1);

    private volatile boolean isAvailable = true;

    private ThreeParameterPredicate<Integer, Long, Long> predicate;

    public KeyIntFragment(HashStringAlgorithm hashStringAlgorithm, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        this.hashStringAlgorithm = hashStringAlgorithm;
        this.predicate = predicate;
    }

    public boolean getAndSet(Object key, ThreeParameterPredicate<Integer, Long, Long> pre) {
        final int hash = hashStringAlgorithm.getHash(key);
        setByHash(hash);
        final long groupCount = hyperLogLog.size();
        int sum = getSum(hash);
        return calculation(pre, sum, groupCount);
    }

    public void setByHash(int hash) {
        if (!isAvailable) {
            return;
        }
        
        final int hashIndex = hash & TABLE_MAX_INDEX;
        hyperLogLog.add(hash);
        
        // 使用原子操作增加计数
        int currentValue = keyHashFragment.get(hashIndex);
        if (currentValue > Integer.MAX_VALUE >> 1) {
            reBaseTable();
        }
        allTimes.incrementAndGet();
        keyHashFragment.incrementAndGet(hashIndex);
    }

    public void set(Object key) {
        final int hash = hashStringAlgorithm.getHash(key);
        setByHash(hash);
    }

    /**
     * double check 进行读代级检查，不可读清除数据
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
            return pre.hotKeyRule(sum, allTimes.get(), groupCount);
        }
        if (predicate != null) {
            return predicate.hotKeyRule(sum, allTimes.get(), groupCount);
        }
        return sum > (allTimes.get() / groupCount) >>> 1;
    }

    /**
     * double check 进行读代级检查，不可读清除数据
     */
    public boolean get(Object key) {
        if (!isAvailable) {
            return false;
        }
        return get(key, null);
    }

    public long keySum() {
        return hyperLogLog.size();
    }

    public long getNumberOfTimes() {
        return allTimes.get();
    }

    /**
     * 重平衡操作，需要加锁确保原子性
     */
    private void reBaseTable() {
        // 使用tryLock避免死锁，如果获取不到锁说明其他线程正在重平衡
        if (!rebalanceLock.tryLock()) {
            return;
        }
        
        try {
            // 双重检查，避免重复重平衡
            boolean needRebalance = false;
            for (int i = 0; i < keyHashFragment.length(); i++) {
                if (keyHashFragment.get(i) > Integer.MAX_VALUE >> 1) {
                    needRebalance = true;
                    break;
                }
            }
            
            if (!needRebalance) {
                return;
            }

            lostTimes.incrementAndGet();
            for (int i = 0; i < keyHashFragment.length(); i++) {
                int value = keyHashFragment.get(i);
                if (value == 0) {
                    continue;
                }
                // 使用原子操作更新值
                keyHashFragment.set(i, value >> 1);
            }
        } finally {
            rebalanceLock.unlock();
        }
    }

    public int getSum(int hash) {
        return keyHashFragment.get(hash & TABLE_MAX_INDEX) * (int)lostTimes.get();
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
                        // 使用原子操作重置所有值
                        for (int i = 0; i < keyHashFragment.length(); i++) {
                            keyHashFragment.set(i, 0);
                        }
                        this.hyperLogLog.clear();
                        isAvailable = false;
                        allTimes.set(0);
                        lostTimes.set(1);
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