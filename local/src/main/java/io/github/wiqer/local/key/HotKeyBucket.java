package io.github.wiqer.local.key;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import io.github.wiqer.local.hash.MurmurHash3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 热键检测桶，适用于高频、高精度场景
 * 特点：
 * 1. 使用分片计数，减少竞争
 * 2. 支持动态扩容
 * 3. 高精度计数
 * 4. 并发安全
 */
public class HotKeyBucket {
    private static final int DEFAULT_FRAGMENT_SIZE = 16;
    private static final int MAX_FRAGMENT_SIZE = 64;
    private static final double LOAD_FACTOR = 0.75;
    
    private final ConcurrentHashMap<Integer, KeyIntFragment> fragments;
    private final AtomicInteger fragmentCount;
    private final AtomicLong totalCount;
    private final ReentrantLock resizeLock;
    
    private final HashStringAlgorithm hashAlgorithm;
    private final ThreeParameterPredicate<Integer, Long, Long> hotKeyPredicate;
    
    public HotKeyBucket() {
        this(new MurmurHash3(), null);
    }
    
    public HotKeyBucket(HashStringAlgorithm hashAlgorithm, ThreeParameterPredicate<Integer, Long, Long> hotKeyPredicate) {
        this.fragments = new ConcurrentHashMap<>();
        this.fragmentCount = new AtomicInteger(0);
        this.totalCount = new AtomicLong(0);
        this.resizeLock = new ReentrantLock();
        this.hashAlgorithm = hashAlgorithm;
        this.hotKeyPredicate = hotKeyPredicate;
        
        // 初始化默认分片
        for (int i = 0; i < DEFAULT_FRAGMENT_SIZE; i++) {
            fragments.put(i, new KeyIntFragment(hashAlgorithm, hotKeyPredicate));
        }
        fragmentCount.set(DEFAULT_FRAGMENT_SIZE);
    }
    
    /**
     * 记录键的访问
     * @param key 要记录的键
     * @return 是否是热键
     */
    public boolean record(Object key) {
        if (key == null) {
            return false;
        }
        
        int hash = hashAlgorithm.getHash(key);
        KeyIntFragment fragment = getFragment(hash);
        
        totalCount.incrementAndGet();
        return fragment.getAndSet(key);
    }
    
    /**
     * 检查键是否是热键
     * @param key 要检查的键
     * @return 是否是热键
     */
    public boolean isHotKey(Object key) {
        if (key == null) {
            return false;
        }
        
        int hash = hashAlgorithm.getHash(key);
        KeyIntFragment fragment = getFragment(hash);
        return fragment.get(key);
    }
    
    /**
     * 获取键的访问次数
     * @param key 要查询的键
     * @return 访问次数
     */
    public long getCount(Object key) {
        if (key == null) {
            return 0;
        }
        
        int hash = hashAlgorithm.getHash(key);
        KeyIntFragment fragment = getFragment(hash);
        return fragment.getSum(hash);
    }
    
    /**
     * 获取总访问次数
     * @return 总访问次数
     */
    public long getTotalCount() {
        return totalCount.get();
    }
    
    /**
     * 获取分片数量
     * @return 分片数量
     */
    public int getFragmentCount() {
        return fragmentCount.get();
    }
    
    /**
     * 清理所有数据
     */
    public void clear() {
        fragments.values().forEach(KeyIntFragment::clear);
        totalCount.set(0);
    }
    
    /**
     * 获取或创建分片
     * @param hash 键的哈希值
     * @return 对应的分片
     */
    private KeyIntFragment getFragment(int hash) {
        int currentSize = fragmentCount.get();
        int index = Math.abs(hash % currentSize);
        
        KeyIntFragment fragment = fragments.get(index);
        if (fragment != null) {
            return fragment;
        }
        
        // 需要扩容
        if (currentSize < MAX_FRAGMENT_SIZE && 
            (double)totalCount.get() / currentSize > LOAD_FACTOR) {
            resize();
            // 重新计算索引
            index = Math.abs(hash % fragmentCount.get());
        }
        
        // 双重检查
        fragment = fragments.get(index);
        if (fragment != null) {
            return fragment;
        }
        
        // 创建新分片
        fragment = new KeyIntFragment(hashAlgorithm, hotKeyPredicate);
        KeyIntFragment existing = fragments.putIfAbsent(index, fragment);
        return existing != null ? existing : fragment;
    }
    
    /**
     * 扩容分片
     */
    private void resize() {
        if (!resizeLock.tryLock()) {
            return;
        }
        
        try {
            int currentSize = fragmentCount.get();
            if (currentSize >= MAX_FRAGMENT_SIZE) {
                return;
            }
            
            int newSize = Math.min(currentSize * 2, MAX_FRAGMENT_SIZE);
            for (int i = currentSize; i < newSize; i++) {
                fragments.put(i, new KeyIntFragment(hashAlgorithm, hotKeyPredicate));
            }
            fragmentCount.set(newSize);
        } finally {
            resizeLock.unlock();
        }
    }
} 