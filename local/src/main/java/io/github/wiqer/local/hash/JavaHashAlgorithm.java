package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashAlgorithm implements HashAlgorithm {
    @Override
    public Integer getHash(Object key) {
        return key.hashCode();
    }

    @Override
    public Integer hash(String key) {
        return key.hashCode();
    }

    @Override
    public Integer skipPrefixHash(String key, String prefix) {
        return key.substring(prefix.length()).hashCode();
    }


    @Override
    public int skipPrefixHash(String key, String prefix, int tableHixSize) {
        return skipPrefixHash(key,prefix)  & tableHixSize;
    }

    @Override
    public int getHashIndex(int hash, int tableHixSize) {
        return hash & tableHixSize;
    }
}
