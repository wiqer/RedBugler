package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.HashStringAlgorithm;

import java.util.List;

public interface HashAlgorithmGroup {

    HashStringAlgorithm getByName(String name);

    HashStringAlgorithm put(String name, HashStringAlgorithm defaultAlgorithm);

    List<HashStringAlgorithm> getAllAlgorithms();
}
