package io.github.wiqer.local.hash;

public class AbcHashAlgorithm implements HashAlgorithm {
    @Override
    public Integer getHash(Object key) {
        return hash(key.toString());
    }

    @Override
    public Integer hash(String key) {
        int hash = 0;
        int checkSize = Math.min(key.length(), 64);
        for (int i = 0; i < checkSize; i++) {
            final char value =key.charAt(i);
            hash = getHash(hash, value);
        }
        return hash;
    }

    @Override
    public Integer skipPrefixHash(String key, String prefix) {
        int keyLen = key.length();
        int prefixLen = prefix.length();
        if(keyLen < prefix.length()){
            return 0;
        }
        int hash = 0;
        int checkSize = Math.min(keyLen - prefixLen, 64);
        for (int i = prefixLen -1 ; i < checkSize; i++) {
            char value =key.charAt(i);
            hash = getHash(hash, value);
        }
        return hash;
    }

    @Override
    public int skipPrefixHash(String key, String prefix, int tableHixSize) {
        return (skipPrefixHash(key,prefix) >> 4) & tableHixSize;
    }

    @Override
    public int getHashIndex(int hash, int tableHixSize) {
        return (hash >> 4) & tableHixSize;
    }

    private int getHash(int hash, char value) {
        if(value > 0x38 && value < 0x7f) {
            hash += hash << 6 + (value - 0x38);
        }else {
            hash += value ;
            hash = (hash ^ hash >>> 7) * -41;
            hash = (hash ^ hash >>> 7) * -37;
            hash = hash ^ hash >>> 7;
        }
        return hash;
    }
}