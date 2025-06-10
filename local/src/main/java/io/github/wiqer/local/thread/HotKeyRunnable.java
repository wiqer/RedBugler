package io.github.wiqer.local.thread;

import io.github.wiqer.local.counter.HotKeyByteBucket;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class HotKeyRunnable implements Runnable{

    @EqualsAndHashCode.Exclude
    private final HotKeyByteBucket bucket;

    @EqualsAndHashCode.Include
    private final int bucketIndex;

    @EqualsAndHashCode.Include
    private final Object key;

    public HotKeyRunnable(HotKeyByteBucket bucket, int bucketIndex, Object key) {
        this.bucket = bucket;
        this.bucketIndex = bucketIndex;
        this.key = key;
    }

    @Override
    public void run() {
        bucket.set(key);
    }
}
