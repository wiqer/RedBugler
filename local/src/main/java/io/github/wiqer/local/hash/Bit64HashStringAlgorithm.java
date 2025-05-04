package io.github.wiqer.local.hash;

public class Bit64HashStringAlgorithm implements HashStringAlgorithm {
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

}