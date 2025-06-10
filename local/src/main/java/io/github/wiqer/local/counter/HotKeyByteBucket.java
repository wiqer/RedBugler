package io.github.wiqer.local.counter;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.key.KeyByteFragment;
import io.github.wiqer.local.key.ThreeParameterPredicate;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HotKeyByteBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    @Getter
    private final long id;

    private final List<KeyByteFragment> keyByteFragmentArray;

    /**
     * 状态定义：
     * 0 - 可用状态
     * 1 - 正在删除
     * 2 - 已删除完成
     */
    @Getter
    private final AtomicInteger status = new AtomicInteger(0);

    /**
     * 先存主存储，满了存储到multithreadedCacheBackUp，并通知消费线程去消费
     * <p>
     * private ThreadPoolExecutor threadPoolExecutor;
     * <p>
     * private volatile long runThreadId;
     * <p>
     * /**
     * | **** |0 可用| 2 已删除完成| 1 正在删除|当前活动时期的运行 add状态，0 未运行，1，正在运行|
     */

    public HotKeyByteBucket(List<HashStringAlgorithm> hashStringAlgorithmList, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        this.id = nextId.getAndIncrement();
        this.keyByteFragmentArray = hashStringAlgorithmList.stream()
                .map(algorithm -> new KeyByteFragment(algorithm, predicate))
                .collect(Collectors.toList());
    }

    public boolean getAndSet(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        // 如果正在删除，等待删除完成
        if (status.get() == 1) {
            return false;
        }
        
        // 尝试将状态设置为可用
        if (!status.compareAndSet(2, 0)) {
            return false;
        }

        boolean result = true;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
            result &= keyByteFragment.getAndSet(key, predicate);
        }
        return result;
    }

    public void set(Object key) {
        // 如果正在删除，等待删除完成
        if (status.get() == 1) {
            return;
        }
        
        // 尝试将状态设置为可用
        if (!status.compareAndSet(2, 0)) {
            return;
        }

        for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
            keyByteFragment.set(key);
        }
    }

    public boolean getAndSet(Object key) {
        return getAndSet(key, null);
    }

    public boolean get(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        // 如果正在删除或已删除完成，直接返回false
        if (status.get() != 0) {
            return false;
        }

        boolean result = true;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
            result &= keyByteFragment.get(key, predicate);
        }
        return result;
    }

    public boolean get(Object key) {
        return get(key, null);
    }

    /**
     * 清理数据
     * 使用原子操作确保状态转换的正确性
     */
    public void clear() {
        // 尝试将状态从可用(0)转换为正在删除(1)
        if (status.compareAndSet(0, 1)) {
            try {
                for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
                    keyByteFragment.clear();
                }
            } finally {
                // 无论清理是否成功，都将状态设置为已删除完成(2)
                status.set(2);
            }
        }
    }
}
