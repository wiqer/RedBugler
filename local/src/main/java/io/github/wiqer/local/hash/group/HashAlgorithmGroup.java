package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.HashAlgorithm;

public interface HashAlgorithmGroup {

    HashAlgorithm getByName(String name);

}
