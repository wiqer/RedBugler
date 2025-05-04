package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.key.KeyByteFragment;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HotKeyRuleBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    @Getter
    private final long id;

    private final List<KeyByteFragment> keyByteFragmentArray;

    /**
     * 先存主存储，满了存储到multithreadedCacheBackUp，并通知消费线程去消费

    private ThreadPoolExecutor threadPoolExecutor;

    private volatile long runThreadId;

    /**
     * | **** |0 可用| 0 已删除完成 ，1 正在删除|当前活动时期的运行 add状态，0 未运行，1，正在运行|
     */
    @Getter
    private volatile int status = 0;


    public HotKeyRuleBucket(List<HashStringAlgorithm> hashStringAlgorithmList) {
        this.id = nextId.getAndIncrement();
        this.keyByteFragmentArray = hashStringAlgorithmList.stream().map(KeyByteFragment::new).collect(Collectors.toList());
    }


    public boolean getAndSet(Object key){
        boolean result = true;
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            result &= keyByteFragment.getAndSet(key);
        }
        return result;
    }


    public void clear(){
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            keyByteFragment.clear();
        }
    }

}
