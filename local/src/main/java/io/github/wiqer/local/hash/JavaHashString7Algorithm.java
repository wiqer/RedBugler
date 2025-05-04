package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashString7Algorithm implements HashStringAlgorithm {
    @Override
    public Integer getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 7) | (h << (25)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public Integer hash(String key) {
        return getHash(key);
    }

}
