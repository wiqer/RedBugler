package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashString5Algorithm implements HashStringAlgorithm {
    @Override
    public Integer getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> 5) | (h << (27)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public Integer hash(String key) {
        return getHash(key);
    }

    @Override
    public Boolean isFast(){
        return true;
    }
}
