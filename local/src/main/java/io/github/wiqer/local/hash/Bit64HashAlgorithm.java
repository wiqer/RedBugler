package io.github.wiqer.local.hash;

public class Bit64HashAlgorithm implements HashAlgorithm {
    @Override
    public Integer getHash(Object key) {
        return hash(key.toString());
    }

    @Override
    public Integer hash(String key) {
        int hash = 0;
        int checkSize = Math.min(key.length(), 64);
        for (int i = 0; i < checkSize; i++) {
            final char value = (char) ((key.charAt(i) - '0') & 63);
            hash += hash << 4;
            hash += value;

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
            final char value = (char) ((key.charAt(i) - '0') & 63);
            hash += hash << 4;
            hash += value;

        }
        return hash;
    }


    @Override
    public int skipPrefixHash(String key, String prefix, int tableHixSize) {
        return (skipPrefixHash(key,prefix) >> 1) & tableHixSize;
    }

    @Override
    public int getHashIndex(int hash, int tableHixSize) {
        return (hash >> 1) & tableHixSize;
    }
}