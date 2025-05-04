package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.key.KeyByteFragment;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class HotKeyRuleBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    private final long id;

    private KeyByteFragment[] keyByteFragmentArray;

    /**
     * 先存主存储，满了存储到multithreadedCacheBackUp，并通知消费线程去消费

    private ThreadPoolExecutor threadPoolExecutor;

    private volatile long runThreadId;

    /**
     * | **** |0 可用| 0 已删除完成 ，1 正在删除|当前活动时期的运行 add状态，0 未运行，1，正在运行|
     */
    private volatile int status = 0;

    public HotKeyRuleBucket(HashStringAlgorithm[] hashStringAlgorithmArray) {
        this.id = nextId.getAndIncrement();
        keyByteFragmentArray = new KeyByteFragment[hashStringAlgorithmArray.length];
        for (int i = 0; i < hashStringAlgorithmArray.length; i++) {
            keyByteFragmentArray[i] = new KeyByteFragment(hashStringAlgorithmArray[i]);
        }
    }

    public HotKeyRuleBucket(HashStringAlgorithm[] hashStringAlgorithmArray, String prefix, ThreadPoolExecutor threadPoolExecutor, Integer queueSize) {
        this.id = nextId.getAndIncrement();
        keyByteFragmentArray = new KeyByteFragment[hashStringAlgorithmArray.length];
        for (int i = 0; i < hashStringAlgorithmArray.length; i++) {
            keyByteFragmentArray[i] = new KeyByteFragment(hashStringAlgorithmArray[i]);
        }
    }
    public HotKeyRuleBucket() {
        this.id = nextId.getAndIncrement();
    }

    public long getId() {
        return id;
    }

    public int getStatus() {
        return status;
    }


    public boolean getAndSet(Object key){
        boolean result = true;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            result &= keyByteFragment.getAndSet(key);
        }
        return result;
    }


    public void clear(int era){
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            keyByteFragment.swapAndClear(era);
        }
    }

}
