package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashString4Algorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 4) | (h << (28)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public boolean isFast(){
        return true;
    }
}
