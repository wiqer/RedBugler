package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashAlgorithm;
import io.github.wiqer.local.key.KeyByteFragment;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class HotKeyRuleBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    private final long id;

    private KeyByteFragment[] keyByteFragmentArray;

    private HashAlgorithm[] hashAlgorithmArray ;

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

    public HotKeyRuleBucket(HashAlgorithm[] hashAlgorithmArray) {
        this.id = nextId.getAndIncrement();
        this.hashAlgorithmArray = hashAlgorithmArray;;
        keyByteFragmentArray = new KeyByteFragment[hashAlgorithmArray.length];
        for (int i = 0; i < hashAlgorithmArray.length; i++) {
            keyByteFragmentArray[i] = new KeyByteFragment(hashAlgorithmArray[i]);
        }
    }

    public HotKeyRuleBucket(HashAlgorithm[] hashAlgorithmArray, String prefix, ThreadPoolExecutor threadPoolExecutor, Integer queueSize) {
        this.id = nextId.getAndIncrement();
        this.hashAlgorithmArray = hashAlgorithmArray;
        keyByteFragmentArray = new KeyByteFragment[hashAlgorithmArray.length];
        for (int i = 0; i < hashAlgorithmArray.length; i++) {
            keyByteFragmentArray[i] = new KeyByteFragment(hashAlgorithmArray[i]);
        }
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

    public int getStatus() {
        return status;
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
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            if(keyByteFragment.getHashAlgorithm(hashAlgorithm)){
                keyByteFragment.add(hash);
                return true;
            }
        }
        return false;
    }

    public boolean add(int hash, int hashAlgorithmIndex){
        KeyByteFragment keyByteFragment =  keyByteFragmentArray[hashAlgorithmIndex];
        keyByteFragment.add(hash);
        return true;
    }
    public int get(int hash, HashAlgorithm hashAlgorithm){
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            if(keyByteFragment.getHashAlgorithm(hashAlgorithm)){
                return keyByteFragment.get(hash);
            }
        }
        return 0;
    }


    public int getByAlgorithmIndex(int hash, int hashAlgorithmIndex){
        return keyByteFragmentArray[hashAlgorithmIndex].get(hash);
    }

    public int getKeySumByAlgorithmIndex(int hashAlgorithmIndex){
        return keyByteFragmentArray[hashAlgorithmIndex].keySum();
    }

    public long getNumberOfTimesByAlgorithmIndex(int hashAlgorithmIndex){
        return keyByteFragmentArray[hashAlgorithmIndex].getNumberOfTimes();
    }

    public int getTimesSumByAlgorithmIndex(int hash, int hashAlgorithmIndex){
        return keyByteFragmentArray[hashAlgorithmIndex].get(hash);
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
            for (KeyByteFragment kbf : keyByteFragmentArray){
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

    /**
     * 异步删除
     */
    public void clear(int era) {
        /**
         * 没有任务清理，发起任务清理
         */

        if((status & 0b110) == 0){
            synchronized (this){
                if((status & 0b110) == 0){
                    status = status|0b110;
                    threadPoolExecutor.execute(this.clearRun(era));
                }
            }
        }
    }


    public Runnable clearRun(int era) {
        if((status & 0b110) == 0){
            synchronized (this){
                if((status & 0b110) != 0){
                    return null;
                }
                status = status|0b110;
            }
        }
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            keyByteFragment.swapAndClear(era);
        }
        if((status & 0b110) == 0b110){
            synchronized (this){
                if((status & 0b110) == 0b110){
                    status = status & 0xFF9;
                }
            }
        }
        return null;
    }

    public void synchronousClear(int era){
        for (KeyByteFragment keyByteFragment : keyByteFragmentArray){
            keyByteFragment.swapAndClear(era);
        }
    }

}
