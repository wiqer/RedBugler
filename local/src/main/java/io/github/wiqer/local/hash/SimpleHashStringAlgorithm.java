package io.github.wiqer.local.hash;

/**
 * 简单哈希算法实现
 * 特点：
 * 1. 实现简单
 * 2. 性能最好
 * 3. 分布一般
 */
public class SimpleHashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        if (key == null) {
            return 0;
        }
        
        String str = key.toString();
        int hash = 0;
        
        for (int i = 0; i < str.length(); i++) {
            hash = hash * 31 + str.charAt(i);
        }
        
        return hash;
    }
}