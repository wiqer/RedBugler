package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashString3Algorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 3) | (h << (29)) ;
        return (h ) ^ (h >>> 16);
    }
}
