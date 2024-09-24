package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashAlgorithm;
import io.github.wiqer.local.hash.group.HashAlgorithmGroup;
import io.github.wiqer.local.key.KeyByteFragment;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HotKeyRuleBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    private final long id;

    private List<KeyByteFragment> keyByteFragmentList;

    private List<HashAlgorithm> hashAlgorithm ;

    private String prefix;
    /**
     * 先存主存储，满了存储到multithreadedCacheBackUp，并通知消费线程去消费
     */
    private ArrayBlockingQueue<String> multithreadedCache ;

    private ArrayBlockingQueue<String> multithreadedCacheBackUp;

    private ThreadPoolExecutor threadPoolExecutor;

    private volatile long runThreadId;

    /**
     * | **** |0 可用| 0 已删除完成 ，1 正在删除|当前活动时期的运行 add状态，0 未运行，1，正在运行|
     */
    private volatile int status = 0;

    public HotKeyRuleBucket(List<HashAlgorithm> hashAlgorithm) {
        this.id = nextId.getAndIncrement();
        this.hashAlgorithm = hashAlgorithm.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());;
        this.keyByteFragmentList = hashAlgorithm.stream().map(KeyByteFragment::new).collect(Collectors.toList());
    }

    public HotKeyRuleBucket(HashAlgorithmGroup hashAlgorithmGroup, List<String> hashAlgorithmNameList, String prefix, ThreadPoolExecutor threadPoolExecutor, Integer queueSize) {
        this.id = nextId.getAndIncrement();
        this.hashAlgorithm = hashAlgorithmNameList.stream()
                .map(hashAlgorithmGroup::getByName)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        this.keyByteFragmentList = hashAlgorithm.stream()
                .map(KeyByteFragment::new)
                .collect(Collectors.toList());
        this.threadPoolExecutor = threadPoolExecutor;
        if (queueSize == null) {
            queueSize = 4096;
        }
        this.multithreadedCache = new ArrayBlockingQueue<String> (queueSize << 1);
        this.multithreadedCacheBackUp = new ArrayBlockingQueue<String> (queueSize << 1);
        this.prefix = prefix;

    }
    public HotKeyRuleBucket() {
        this.id = nextId.getAndIncrement();
    }

    public long getId() {
        return id;
    }

    @Deprecated
    public boolean add(String key){
        if(multithreadedCache.offer(key)){
            return true;
        }else if(multithreadedCacheBackUp.offer(key)){
            if((status & 1) == 0){
                synchronized (this){
                    if((status & 1) == 0){
                        status = status|1;
                        threadPoolExecutor.execute(this::concurrentRun);
                    }
                }
            }
            return true;
        }
        /**
         * 处理不了的直接丢弃了，防止内存和cpu全崩溃
         */
        else return false;
    }

    public boolean getAndSet(int hash, HashAlgorithm hashAlgorithm){
        for (KeyByteFragment keyByteFragment : keyByteFragmentList){
            if(keyByteFragment.getHashAlgorithm(hashAlgorithm)){
                keyByteFragment.add(hash);
                return true;
            }
        }
        return false;
    }

    public int get(int hash, HashAlgorithm hashAlgorithm){
        for (KeyByteFragment keyByteFragment : keyByteFragmentList){
            if(keyByteFragment.getHashAlgorithm(hashAlgorithm)){
                return keyByteFragment.get(hash);
            }
        }
        return 0;
    }

    private void concurrentRun(){
        if((status & 1) == 0){
            synchronized (this){
                if((status & 1) == 0){
                    status = status|1;
                }else {
                    return;
                }
            }
        }else {
            return;
        }

        String key = multithreadedCache.poll();
        while (key != null){
           multithreadedCacheBackUp.offer(key);
           key = multithreadedCache.poll();
        }
        key = multithreadedCacheBackUp.poll();
        while (key != null){
            for (KeyByteFragment kbf : keyByteFragmentList){
                kbf.add(key, prefix);
            }
            key = multithreadedCacheBackUp.poll();
        }

        if((status & 1) == 1){
            synchronized (this){
                if((status & 1) == 1){
                    status = status & 0xFFE;
                }
            }
        }
    }

    public void clear() {
        /**
         * 没有任务清理，发起任务清理
         */
        if((status & 0b110) == 0){
            synchronized (this){
                if((status & 0b110) == 0){
                    status = status|0b110;
                    threadPoolExecutor.execute(this::clearRun);
                }
            }
        }
    }


    public void clearRun() {
        if((status & 0b110) == 0){
            synchronized (this){
                if((status & 0b110) != 0){
                    return;
                }
                status = status|0b110;
            }
        }
        for (KeyByteFragment keyByteFragment : keyByteFragmentList){
            keyByteFragment.clear();
        }
        if((status & 1) == 1){
            synchronized (this){
                if((status & 1) == 1){
                    status = status & 0xFF9;
                }
            }
        }
    }
}
