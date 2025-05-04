package io.github.wiqer.local.hash;


public class RandomHashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public Integer getHash(Object key) {
        return hash(key.toString());
    }

    @Override
    public Integer hash(String key) {
        int hash = 0;
        int checkSize = Math.min(key.length(), 64);
        for (int i = 0; i < checkSize; i++) {
            final char value = key.charAt(i);
            hash += value;
            hash = (hash ^ hash >>> 7) * -41;
            hash = (hash ^ hash >>> 7) * -37;
        }
        hash += key.length();//9988531 -> 9988536
        return hash;
    }


}