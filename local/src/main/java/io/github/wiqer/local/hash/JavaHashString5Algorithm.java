package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashString5Algorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 5) | (h << (27)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public boolean isFast(){
        return true;
    }
}
