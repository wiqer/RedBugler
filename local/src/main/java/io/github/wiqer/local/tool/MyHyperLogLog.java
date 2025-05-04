package io.github.wiqer.local.tool;

import lombok.Getter;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

import java.util.concurrent.locks.ReentrantLock;

public class MyHyperLogLog {
    @Getter
    private final HLL hyperLogLog;
    private int log2m = 4;
    volatile long writeTime = -1;
    volatile long readTime = -1;
    volatile long count = 0;
    private final ReentrantLock lock = new ReentrantLock();

    public MyHyperLogLog(int nubSize) {
        if (nubSize < 100) {
            throw new IllegalArgumentException("nubSize must be greater than or equal to 100");
        }
        this.log2m = (int) Math.max((getIntBitsNumber(nubSize) >> 2) - 1, 4);
        this.hyperLogLog = new HLL(log2m, 5, -1, true, HLLType.EMPTY);

    }

    public void add(int value) {
        writeTime = SystemClock.now();
        lock.lock();
        try {
            hyperLogLog.addRaw(value);
        } finally {
            lock.unlock();
        }

    }

    public void add(MyHyperLogLog other) {
        writeTime = SystemClock.now();
        lock.lock();
        try {
            hyperLogLog.union(other.getHyperLogLog());
        } finally {
            lock.unlock();
        }

    }

    public void clear() {
        lock.lock();
        try {
            hyperLogLog.clear();
        } finally {
            lock.unlock();
        }

    }

    private static long getIntBitsNumber(long number) {
        int bitsnumber = 0;
        number = number & Integer.MAX_VALUE;
        while (number > 0) {
            bitsnumber++;
            number = number >> 1;
        }
        return bitsnumber;
    }

    public Long size() {
        if (writeTime > readTime) {
            readTime = SystemClock.now();
            count = cardinality();
        }
        return count;
    }

    public long cardinality() {
        lock.lock();
        try {
            return hyperLogLog.cardinality();
        } finally {
            lock.unlock();
        }
    }
}
