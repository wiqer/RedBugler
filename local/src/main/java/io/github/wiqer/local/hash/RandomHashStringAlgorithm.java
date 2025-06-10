package io.github.wiqer.local.hash;


public class RandomHashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        String str = key.toString();
        int hash = 0;
        int checkSize = Math.min(str.length(), 64);
        for (int i = 0; i < checkSize; i++) {
            final char value = str.charAt(i);
            hash += value;
            hash = (hash ^ hash >>> 7) * -41;
            hash = (hash ^ hash >>> 7) * -37;
        }
        hash += str.length();//9988531 -> 9988536
        return hash;
    }
}