package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.HashStringAlgorithm;

public interface HashAlgorithmGroup {

    HashStringAlgorithm getByName(String name);

}
