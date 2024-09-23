package io.github.wiqer.local.hash;

/**
 * @author MultipleHashHotkeyCache 中对key取特征值的hash算法
 */
public interface HashAlgorithm {

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
    public Integer hash(String key);

    /**
     * 放弃前缀。生产特征值
     * @param key
     * @return
     */
    public Integer skipPrefixHash(String key,String prefix);

    public int skipPrefixHash(String key,String prefix, int tableHixSize);

    public int getHashIndex(int hash, int tableHixSize);

}
