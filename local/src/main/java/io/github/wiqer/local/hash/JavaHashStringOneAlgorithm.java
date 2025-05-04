package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashStringOneAlgorithm implements HashStringAlgorithm {
    @Override
    public Integer getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 1) | (h << (31)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public Integer hash(String key) {
        int h = key.hashCode();
        h = (h >>> 1) | (h << (31)) ;
        return (h ) ^ (h >>> 16);
    }

}
