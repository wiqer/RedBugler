package io.github.wiqer.local.hash;

/**
 * Java默认哈希算法实现
 * 特点：
 * 1. 使用Object.hashCode()
 * 2. 性能较好
 * 3. 分布一般
 */
public class JavaHashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        return key == null ? 0 : key.hashCode();
    }

    @Override
    public boolean isFast(){
        return true;
    }
}
