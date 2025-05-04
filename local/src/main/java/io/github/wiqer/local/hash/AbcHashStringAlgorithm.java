package io.github.wiqer.local.hash;

public class AbcHashStringAlgorithm implements HashStringAlgorithm {
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