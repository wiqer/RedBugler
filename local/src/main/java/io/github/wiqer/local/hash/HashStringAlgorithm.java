package io.github.wiqer.local.hash;

/**
 * @author MultipleHashHotkeyCache 中对key取特征值的hash算法
 */
public interface HashStringAlgorithm extends HashAlgorithm<String> {

    /**
     * 生产特征值
     * @param key
     * @return
     */
    Integer getHash(Object key);

    /**
     * 生产特征值
     * @param key
     * @return
     */
    Integer hash(String key);

    default Boolean isFast(){
        return false;
    }

}
