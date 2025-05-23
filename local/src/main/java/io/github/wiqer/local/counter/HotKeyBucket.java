package io.github.wiqer.local.counter;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.key.KeyByteFragment;
import io.github.wiqer.local.key.ThreeParameterPredicate;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HotKeyBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    @Getter
    private final long id;

    private final List<KeyByteFragment> keyByteFragmentArray;

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
    @Getter
    private volatile int status = 0;


    public HotKeyBucket(List<HashStringAlgorithm> hashStringAlgorithmList, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        this.id = nextId.getAndIncrement();
        this.keyByteFragmentArray = hashStringAlgorithmList.stream().map(algorithm -> new KeyByteFragment(algorithm, predicate)).collect(Collectors.toList());
    }

    public boolean getAndSet(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        if (status == 1) {
            for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
                keyByteFragment.clear();
            }
            status = 2;
        }
        status = 0;
        boolean result = true;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
            result &= keyByteFragment.getAndSet(key, predicate);
        }
        return result;
    }

    public void set(Object key) {
        if (status == 1) {
            for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
                keyByteFragment.clear();
            }
            status = 2;
        }
        status = 0;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
            keyByteFragment.set(key);
        }
    }


    public boolean getAndSet(Object key) {
        return getAndSet(key, null);
    }


    public boolean get(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        boolean result = true;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
            result &= keyByteFragment.get(key, predicate);
        }
        return result;
    }

    public boolean get(Object key) {
        return get(key, null);
    }


    public void clear() {
        if (status == 0) {
            status = 1;
            for (KeyByteFragment keyByteFragment : keyByteFragmentArray) {
                keyByteFragment.clear();
            }
            status = 2;
        }
    }

}
