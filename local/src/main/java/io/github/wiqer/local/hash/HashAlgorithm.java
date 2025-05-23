package io.github.wiqer.local.hash;

/**
 * @author MultipleHashHotkeyCache 中对key取特征值的hash算法
 */
public interface HashAlgorithm<T> {

    /**
     * 生产特征值
     * @param key
     * @return
     */
    public Integer getHash(Object key);

    /**
     * 生产特征值
     * @param key
     * @return
     */
    public Integer hash(T key);

}
