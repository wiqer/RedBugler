package io.github.wiqer.local.rule;

import io.github.wiqer.local.hash.HashAlgorithm;
import io.github.wiqer.local.hash.group.HashAlgorithmGroup;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HotKeyRuleSegmentation {

    /**
     * 分时存储
     */
    private HotKeyRuleBucket[] hotKeyRuleBuckets = new HotKeyRuleBucket[8];

    private int bucketIndexSize = 0x7;

    private List<HashAlgorithm> hashAlgorithmList ;

    private String prefix;
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
        for (int i = 0; i < hotKeyRuleBuckets.length; i++) {
            hotKeyRuleBuckets[i] = new HotKeyRuleBucket(hashAlgorithmList);
        }
        this.prefix = prefix;
        this.timeLevel = timeLevel;
    }

    public void add(String key){
        int index = (int) (getTime()) >> timeLevel & bucketIndexSize;
        int nextIndex = (index + 1) & bucketIndexSize;
        HotKeyRuleBucket hotKeyRuleBucket = hotKeyRuleBuckets[index];
        for (HashAlgorithm hashAlgorithm: hashAlgorithmList){
            int hash = hashAlgorithm.skipPrefixHash(key, prefix);
            for (int k = 0; k < bucketIndexSize; k++){
                HotKeyRuleBucket conurrenHotKeyRuleBucket = hotKeyRuleBuckets[k];
                if(index == k){
                    conurrenHotKeyRuleBucket.getAndSet(hash,hashAlgorithm);
                }else if(nextIndex == k){
                    conurrenHotKeyRuleBucket.clear();
                }else {
                   int hashTimes = conurrenHotKeyRuleBucket.get(hash,hashAlgorithm);
                }
            }
        }

    }

    private long getTime(){
        return System.currentTimeMillis();
    }

}
