package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashAlgorithm;
import io.github.wiqer.local.hash.group.HashAlgorithmGroup;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HotKeyRuleSegmentation {

    private final byte bucketSize = 8;

    private final byte bucketSizeBitSum = (byte) getIntBitsNumber(8);


    /**
     * 分时存储
     */
    private final HotKeyRuleBucket[] hotKeyRuleBuckets = new HotKeyRuleBucket[bucketSize];

    private HashAlgorithm[] hashAlgorithmArray ;

    private List<HashAlgorithm> hashAlgorithmList ;

    private String prefix;

    private Long startTime = System.currentTimeMillis();
    /**
     * 有效的扫描时间
     * 0=>10ms
     * 1=>20ms
     * 2=>40ms
     * 3=>80ms
     * 4=>160ms
     * 以此类推
     */
    private int timeLevel = 0;


    public HotKeyRuleSegmentation(HashAlgorithmGroup hashAlgorithmGroup, List<String> hashAlgorithmNameList, String prefix, int timeLevel) {
        this.hashAlgorithmList = hashAlgorithmNameList.stream()
                .map(hashAlgorithmGroup::getByName)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        hashAlgorithmArray = new HashAlgorithm[hashAlgorithmList.size()];
        hashAlgorithmList.toArray(hashAlgorithmArray);
        for (int i = 0; i < hotKeyRuleBuckets.length; i++) {
            hotKeyRuleBuckets[i] = new HotKeyRuleBucket(hashAlgorithmArray);
        }
        this.prefix = prefix;
        this.timeLevel = timeLevel;
    }

    public boolean add(String key){
        byte bucketIndexSize = bucketSize - 1;
        int era = ((int) (((getTime() - startTime)&Integer.MAX_VALUE))>> (timeLevel + bucketSizeBitSum));
        int index = (int) (getTime()) >> timeLevel & bucketIndexSize;
        int nextIndex = (index + 1) & bucketIndexSize;
        HotKeyRuleBucket hotKeyRuleBucket = hotKeyRuleBuckets[index];
        int isHotFragmentTimes = 0;
        for (int i = 0; i < hashAlgorithmArray.length; i++){
            HashAlgorithm hashAlgorithm = hashAlgorithmArray[i];
            int hash = hashAlgorithm.skipPrefixHash(key, prefix);

            for (int k = 0; k < bucketIndexSize; k++){
                HotKeyRuleBucket conurrenHotKeyRuleBucket = hotKeyRuleBuckets[k];
                if(index == k){
                    conurrenHotKeyRuleBucket.add(hash,i);
                }else if(nextIndex == k){
                    conurrenHotKeyRuleBucket.synchronousClear(era);
                }else {
                    if(conurrenHotKeyRuleBucket.getStatus() != 0){
                        continue;
                    }
                    int hashTimes = conurrenHotKeyRuleBucket.getByAlgorithmIndex(hash,i);
                    int keySum = conurrenHotKeyRuleBucket.getKeySumByAlgorithmIndex(i);
                    long numberOfTimes = conurrenHotKeyRuleBucket.getNumberOfTimesByAlgorithmIndex(i);
                    if(conurrenHotKeyRuleBucket.getStatus() != 0){
                        continue;
                    }
                    /**
                     * todo 根据有基尼系数汇算是否分片热
                     */
                    if(hashTimes > numberOfTimes / keySum){
                        isHotFragmentTimes++;
                    }
                }
            }
        }
        /**
         * 汇算是否分片过半都是热键
         */
        if(isHotFragmentTimes > (hashAlgorithmArray.length << (bucketSize>>1))){
            return true;
        }
        return false;

    }

    private long getTime(){
        return System.currentTimeMillis();
    }

    private static int getIntBitsNumber(int number) {
        int bitsnumber = 0;
        number = number & Integer.MAX_VALUE;
        while (number > 0){
            bitsnumber++;
            number = number >> 1;
        }
        return bitsnumber;
    }


}
