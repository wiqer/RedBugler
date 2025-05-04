package io.github.wiqer.local.hash;


public class Java71HashStringAlgorithm implements HashStringAlgorithm {
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

}