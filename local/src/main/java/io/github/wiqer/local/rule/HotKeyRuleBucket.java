package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashAlgorithm;
import io.github.wiqer.local.hash.group.HashAlgorithmGroup;
import io.github.wiqer.local.key.KeyByteFragment;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HotKeyRuleBucket {

    private static final AtomicLong nextId = new AtomicLong(0);

    private final long id;

    private List<KeyByteFragment> keyByteFragmentList;

    private List<HashAlgorithm> hashAlgorithm ;

    /**
     * 先存主存储，满了存储到multithreadedCacheBackUp，并通知消费线程去消费
     */
    private final BlockingQueue<String> multithreadedCache = new ArrayBlockingQueue<String> (500);
    private final BlockingQueue<String> multithreadedCacheBackUp = new ArrayBlockingQueue<String> (500);

    public HotKeyRuleBucket(List<HashAlgorithm> hashAlgorithm) {
        this.id = nextId.getAndIncrement();
        this.hashAlgorithm = hashAlgorithm;
        this.keyByteFragmentList = hashAlgorithm.stream().map(KeyByteFragment::new).collect(Collectors.toList());
    }

    public HotKeyRuleBucket(HashAlgorithmGroup hashAlgorithmGroup, List<String> hashAlgorithmNameList) {
        this.id = nextId.getAndIncrement();
        this.hashAlgorithm = hashAlgorithmNameList.stream()
                .map(hashAlgorithmGroup::getByName)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        this.keyByteFragmentList = hashAlgorithm.stream()
                .map(KeyByteFragment::new)
                .collect(Collectors.toList());

    }
    public HotKeyRuleBucket() {
        this.id = nextId.getAndIncrement();
    }

    public long getId() {
        return id;
    }
}
