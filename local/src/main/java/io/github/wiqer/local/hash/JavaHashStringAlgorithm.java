package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public Integer getHash(Object key) {
        return key.hashCode();
    }

    @Override
    public Integer hash(String key) {
        return key.hashCode();
    }

    @Override
    public Boolean isFast(){
        return true;
    }
}
