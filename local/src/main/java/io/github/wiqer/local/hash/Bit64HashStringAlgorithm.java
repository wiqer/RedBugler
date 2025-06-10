package io.github.wiqer.local.hash;

/**
 * 64位哈希算法实现
 * 特点：
 * 1. 使用64位哈希
 * 2. 性能较好
 * 3. 分布均匀
 */
public class Bit64HashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        if (key == null) {
            return 0;
        }
        
        String str = key.toString();
        long h = 0x7fffffffffffffffL;
        
        for (int i = 0; i < str.length(); i++) {
            h = h * 31 + str.charAt(i);
        }
        
        return (int)(h & 0x7fffffff);
    }
}