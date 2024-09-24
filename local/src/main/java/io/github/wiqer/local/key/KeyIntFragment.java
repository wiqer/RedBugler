package io.github.wiqer.local.key;

import io.github.karlatemp.unsafeaccessor.Unsafe;
import io.github.wiqer.local.hash.HashAlgorithm;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

public class KeyIntFragment {

    static final int InternalPageSize = Unsafe.getUnsafe().pageSize();
    static final int TableIndexSize = InternalPageSize - 1;
    static final int maxValue = Integer.MIN_VALUE;
    private final HashAlgorithm hashAlgorithm;

    private final int[] keyHashFragment = new int[InternalPageSize];

    HLL myHyperLogLog = new HLL(16,5,-1, true, HLLType.EMPTY);
    private volatile long numberOfTimes  = 0;
    private volatile long lostNumberOfTimes  = 0;


    public KeyIntFragment(HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    public void add(String key, String prefix){
        final int hash = hashAlgorithm.skipPrefixHash(key,prefix);
        final int hashIndex = hashAlgorithm.getHashIndex(hash, TableIndexSize);
       // myHyperLogLog.add(hash);
        int times = keyHashFragment[hashIndex];
        if(times == maxValue){
            lostNumberOfTimes++;
        }else {
            numberOfTimes++;
            keyHashFragment[hashIndex]++;
        }
    }

    public int get(String key, String prefix){
         return keyHashFragment[hashAlgorithm.skipPrefixHash(key,prefix,TableIndexSize)];
    }


}
