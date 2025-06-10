package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashString6Algorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 6) | (h << (26)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public boolean isFast(){
        return true;
    }
}
