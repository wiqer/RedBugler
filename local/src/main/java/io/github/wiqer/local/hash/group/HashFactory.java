package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HashFactory implements HashAlgorithmGroup {
     Map<String, HashStringAlgorithm> map = new ConcurrentHashMap<>(HashType.values().length, 0.5F);

    public HashFactory(){
        for (HashType each : HashType.values()) {
            map.put(each.name(), each.getHashStringAlgorithm());
        }
    }

    @Override
    public HashStringAlgorithm getByName(String name) {
        return map.get(name);
    }

    @Override
    public HashStringAlgorithm put(String name, HashStringAlgorithm defaultAlgorithm) {
        return map.put(name, defaultAlgorithm);
    }

    @Override
    public List<HashStringAlgorithm> getAllAlgorithms() {
        return new ArrayList<>(map.values());
    }


}
