package io.github.wiqer.local.hash;

public class AbcHashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        String str = key.toString();
        int hash = 0;
        int checkSize = Math.min(str.length(), 64);
        for (int i = 0; i < checkSize; i++) {
            final char value = str.charAt(i);
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