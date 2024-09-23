package io.github.wiqer.local.hash;


public class Java71HashAlgorithm implements HashAlgorithm {
    @Override
    public Integer getHash(Object key) {
        return hash(key.toString());
    }

    @Override
    public Integer hash(String key) {
        int highHash = 0;
        int checkSize = Math.min(key.length(), 128);
        for (int i = 0; i < checkSize; i++) {
            final char value = key.charAt(i);
            highHash += value;
            highHash = highHash * -71;
        }
        return highHash;
    }

    @Override
    public Integer skipPrefixHash(String key, String prefix) {
        int keyLen = key.length();
        int prefixLen = prefix.length();
        if(keyLen < prefix.length()){
            return 0;
        }
        int highHash = 0;
        int checkSize = Math.min(key.length() - prefixLen, 128);
        for (int i = prefixLen -1 ; i < checkSize; i++) {
            final char value = key.charAt(i);
            highHash += value;
            highHash = highHash * -71;
        }
        return highHash;
    }


    @Override
    public int skipPrefixHash(String key, String prefix, int tableHixSize) {
        return skipPrefixHash(key,prefix) >> 5 & tableHixSize;
    }

    @Override
    public int getHashIndex(int hash, int tableHixSize) {
        return hash >> 5 & tableHixSize;
    }
}