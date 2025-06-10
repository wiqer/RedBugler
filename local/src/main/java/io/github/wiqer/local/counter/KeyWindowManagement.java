package io.github.wiqer.local.counter;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.hash.group.HashFactory;
import io.github.wiqer.local.key.ThreeParameterPredicate;
import io.github.wiqer.local.thread.HotKeyRunnable;
import io.github.wiqer.local.thread.HotKeyWorker;
import io.github.wiqer.local.thread.HotKeyWorkerImpl;
import io.github.wiqer.local.tool.SystemClock;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * copy from https://gitee.com/jd-platform-opensource/hotkey com.jd.platform.hotkey.worker.tool.SlidingWindow
 * 此处借鉴至SlidingWindow 的滑动窗口思想，这么写比较简单
 */
@Slf4j
public class KeyWindowManagement {
    /**
     * 这个搞太多似乎也没啥意义
     */
    static final int MAXIMUM_CAPACITY = 1 << 8;

    /**
     * 命中多少个时片算热？0.75 是根据正态分布算出来的，大部分业务都符合正态分布
     */
    static final float DEFAULT_LOAD_FACTOR = 0.50f;

    /**
     * 循环队列，就是装多个窗口用，该数量是windowSize的2倍
     */
    private final HotKeyByteBucket[] timeSlices;
    /**
     * 队列的总长度
     */
    private final int timeSliceSize;

    private final int timeSliceMaxIndex;
    /**
     * 每个时间片的时长，以毫秒为单位
     */
    private final long timeMillisPerSlice;
    /**
     * 共有多少个时间片（即窗口长度），他必须是2的幂次方
     */
    private final int windowSize;

    /**
     * 该滑窗的起始创建时间，也就是第一个数据
     */
    private long beginTimestamp;
    /**
     * 最后一个数据的时间戳
     */
    private long lastAddTimestamp;

    /**
     * 在一个完整窗口期内允许通过的最大阈值,这个是最严格的标准
     */
    private final int threshold;

    private final HotKeyWorker writeThread;

    public KeyWindowManagement(int windowSize, int timeSlice, TimeUnit timeUnit) {
        this(new HashFactory().getAllFastAlgorithms(), windowSize, timeSlice, timeUnit, DEFAULT_LOAD_FACTOR, new HotKeyWorkerImpl("", throwable -> log.error(throwable.getMessage()), Integer.MAX_VALUE >>> 4), null);
    }

    /**
     * 主要还是调用这个接口吧，让用户知道具体原理
     *
     * @param algorithms
     * @param windowSize
     * @param timeSlice
     * @param timeUnit
     * @param loadFactor
     */
    public KeyWindowManagement(List<HashStringAlgorithm> algorithms, int windowSize, int timeSlice, TimeUnit timeUnit, float loadFactor, HotKeyWorker writeThread, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);
        if (timeUnit.equals(TimeUnit.NANOSECONDS)) {
            throw new IllegalArgumentException("no support TimeUnit.NANOSECONDS");
        }
        if (timeUnit.equals(TimeUnit.MICROSECONDS)) {
            throw new IllegalArgumentException("no support TimeUnit.MICROSECONDS");
        }
        this.windowSize = tableSizeFor(windowSize);
        this.timeSliceSize = this.windowSize << 1;
        this.timeMillisPerSlice = timeUnit.toMillis(timeSlice);
        if (this.timeMillisPerSlice < 2) {
            throw new IllegalArgumentException("time millis per slice must > 1  ");
        }
        this.timeSlices = new HotKeyByteBucket[this.timeSliceSize];
        timeSliceMaxIndex = this.timeSliceSize - 1;
        threshold = (int) (this.windowSize * loadFactor);
        for (int i = 0; i < this.timeSliceSize; i++) {
            timeSlices[i] = new HotKeyByteBucket(algorithms, predicate);
        }
        this.writeThread = writeThread;
        writeThread.thread().start();
    }

    /**
     * copy from HashMap
     *
     * @param cap
     * @return
     */
    static final int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /**
     * 计算当前所在的时间片的位置
     */
    private int locationIndex() {
        long now = SystemClock.now();
        //如果当前的key已经超出一整个时间片了，那么就直接初始化就行了，不用去计算了
        if (now - lastAddTimestamp > timeMillisPerSlice * windowSize) {
            reset();
        }

        int index = (int) (((now - beginTimestamp) / timeMillisPerSlice) & timeSliceMaxIndex);
        return Math.max(index, 0);
    }

    /**
     * 初始化
     */
    private synchronized void reset() {
        beginTimestamp = SystemClock.now();
        //窗口个数
        for (int i = 0; i < timeSliceSize; i++) {
            timeSlices[i].clear();
        }
    }

    private void clearFromIndex(int index) {
        for (int i = 1; i <= windowSize; i++) {
            int j = index + i;
            if (j >= (windowSize << 1)){
                j -= (windowSize << 1);
            }
            timeSlices[j].clear();
        }
    }

    private void clearFromIndexSmall(int index) {
        final int max = windowSize + index;
        for (int i = Math.max(1,windowSize>>1) + index; i <= max; i++) {
            if (i >= (windowSize << 1)){
                ;
                timeSlices[i - (windowSize << 1)].clear();
            }else {
                timeSlices[i].clear();
            }

        }
    }

    /**
     * 单线程业务可以使用这个
     *
     * @param key
     * @return
     */
    public synchronized boolean syncGetAndSet(Object key) {
        return get(key);
    }

    private boolean get(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        //当前自己所在的位置，是哪个小时间窗
        int index = locationIndex();
//        System.out.println("index:" + index);
        //然后清空自己前面windowSize到2*windowSize之间的数据格的数据
        //譬如1秒分4个窗口，那么数组共计8个窗口
        //当前index为5时，就清空6、7、8、1。然后把2、3、4、5的加起来就是该窗口内的总和
        clearFromIndex(index);
        int sum = 0;
        sum += timeSlices[index].getAndSet(key, predicate) ? 1 : 0;
        //加上前面几个时间片
        for (int i = 1; i < windowSize; i++) {
            sum += timeSlices[(index - i + timeSliceSize) & timeSliceMaxIndex].get(key,predicate) ? 1 : 0;
        }
        lastAddTimestamp = SystemClock.now();
        int localThreshold = Math.min(1, threshold);
        return sum >= localThreshold;
    }

    private boolean get(Object key) {
        return get(key,null);
    }

    /**
     * 增加count个数量,
     * 混合并发调用，同时读写，可能出现幻觉，对一致性要求高的场景慎用
     */
    public boolean getAndSet(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate) {
        //当前自己所在的位置，是哪个小时间窗
        final int index = locationIndex();
//        System.out.println("index:" + index);
        //然后清空自己前面windowSize到2*windowSize之间的数据格的数据
        //譬如1秒分4个窗口，那么数组共计8个窗口
        //当前index为5时，就清空6、7、8、1。然后把2、3、4、5的加起来就是该窗口内的总和
        clearFromIndexSmall(index);
        int sum = 0;
        //sum += timeSlices[index].getAndSet(key)? 1 : 0;
        final HotKeyByteBucket currentBucket = timeSlices[index];
        writeThread.submit(new HotKeyRunnable(currentBucket, index, key));
        //加上前面几个时间片
        //往后挫一位，减少并发冲突
        //业务意义与get不同，getAndSet不计算当前窗口
        for (int i = 1; i < windowSize + 1; i++) {
            sum += timeSlices[(index - i + timeSliceSize) & timeSliceMaxIndex].get(key) ? 1 : 0;
        }
        lastAddTimestamp = SystemClock.now();
        int localThreshold = Math.min(1, threshold);
        return sum >= localThreshold;
    }

    public boolean getAndSet(Object key) {
        return getAndSet(key, null);
    }

    /**
     * 推荐使用这个调用，读写分离
     *
     * @param key
     * @param timeout
     * @param unit
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */

    public boolean get(Object key, long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        return CompletableFuture.supplyAsync(() -> get(key), writeThread).get(timeout, unit);
    }

    /**
     * 自定义热键定义
     * @param key
     * @param timeout
     * @param unit
     * @param predicate
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public boolean get(Object key, ThreeParameterPredicate<Integer, Long, Long> predicate, long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        return CompletableFuture.supplyAsync(() -> get(key, predicate), writeThread).get(timeout, unit);
    }

}
