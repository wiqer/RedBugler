package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.HashStringAlgorithm;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class HashFactory implements HashAlgorithmGroup {
    static Map<String, HashStringAlgorithm> map = new HashMap<>(HashType.values().length, 0.5F);

    static {
        for (HashType each : HashType.values()) {
            map.put(each.name(), each.getHashStringAlgorithm());
        }
    }

    @Override
    public HashStringAlgorithm getByName(String name) {
        return null;
    }
}
