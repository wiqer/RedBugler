package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashStringTwoAlgorithm implements HashStringAlgorithm {
    @Override
    public Integer getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 2) | (h << (30)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public Integer hash(String key) {
        int h = key.hashCode();
        h = (h >>> 2) | (h << (30)) ;
        return (h ) ^ (h >>> 16);
    }

}
