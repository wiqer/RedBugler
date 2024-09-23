package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashAlgorithm;

public class KeyByteFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TableIndexSize = InternalPageSize - 1;
    private final HashAlgorithm hashAlgorithm;

    private final byte[] keyHashFragment = new byte[InternalPageSize];

    HyperLogLog hyperLogLog = new HyperLogLog();
    private long numberOfTimes  = 0;
    private int lostTimes  = 1;


    public KeyByteFragment(HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    public void add(String key, String prefix){
        final int hash = hashAlgorithm.skipPrefixHash(key,prefix);
        final int hashIndex = hashAlgorithm.getHashIndex(hash, TableIndexSize);
        hyperLogLog.add(hash);
        byte times = keyHashFragment[hashIndex];
        if(times == 127){
            lostTimes++;
            for (int i = 0; i < keyHashFragment.length; i++){
                keyHashFragment[i] = (byte) (keyHashFragment[i]  >> 1);
            }
        }
        numberOfTimes++;
        keyHashFragment[hashIndex]++;
    }

    public int get(String key, String prefix){
         return keyHashFragment[hashAlgorithm.skipPrefixHash(key,prefix,TableIndexSize)] * lostTimes;
    }

    public void clear() {
        this.hyperLogLog.clear();
        numberOfTimes = 0;
        lostTimes = 0;
    }

    public static void main(String[] args) {
        int[] keyHashFragment = new int[InternalPageSize];
        long time = System.currentTimeMillis();
        for (int i = 0; i < 4096; i++) {
            keyHashFragment[i] = (byte) i;
        }
        for (int i = 0; i < 4096; i++) {
            keyHashFragment[i] = (byte) (keyHashFragment[i] / 2);
        }
        for (int j = 0; j < 3000; j++) {

        }

        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }
}
