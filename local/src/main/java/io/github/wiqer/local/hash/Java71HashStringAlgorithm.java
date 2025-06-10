package io.github.wiqer.local.hash;


public class Java71HashStringAlgorithm implements HashStringAlgorithm {
    @Override
    public int getHash(Object key) {
        String str = key.toString();
        int highHash = 0;
        int checkSize = Math.min(str.length(), 128);
        for (int i = 0; i < checkSize; i++) {
            final char value = str.charAt(i);
            highHash += value;
            highHash = highHash * -71;
        }
        return highHash;
    }
}