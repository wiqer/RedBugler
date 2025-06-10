package io.github.wiqer.local.hash;

/**
 * 字符串哈希算法接口
 * 用于计算对象的哈希值
 */
public interface HashStringAlgorithm {
    /**
     * 计算对象的哈希值
     * @param key 要计算哈希值的对象
     * @return 哈希值
     */
    int getHash(Object key);

    /**
     * 判断是否是快速哈希算法
     * @return true if fast, false otherwise
     */
    default boolean isFast() {
        return false;
    }
}
