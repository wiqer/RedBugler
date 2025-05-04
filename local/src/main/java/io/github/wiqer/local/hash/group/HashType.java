package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.*;
import lombok.Getter;


/**
 * 自带的hash算法
 */
@Getter
public enum HashType {
    JH8(new JavaHashStringNAlgorithm(8)), JH9(new JavaHashStringNAlgorithm(9)), JH1(new JavaHashStringOneAlgorithm()), JH2(new JavaHashStringTwoAlgorithm()),
    JH3(new JavaHashString3Algorithm()), JH4(new JavaHashString4Algorithm()), JH5(new JavaHashString5Algorithm()), JH6(new JavaHashString6Algorithm()),
    JH7(new JavaHashString7Algorithm()), JH(new JavaHashStringAlgorithm()), CRC16(new CRC16HashStringAlgorithm()), ABC(new AbcHashStringAlgorithm()),
    BIT64(new Bit64HashStringAlgorithm()), J71H(new Java71HashStringAlgorithm()), RAND(new RandomHashStringAlgorithm()), SIMPLE(new SimpleHashStringAlgorithm()),
    //
    ;

    private final HashStringAlgorithm hashStringAlgorithm;

    HashType(HashStringAlgorithm hashStringAlgorithm) {
        this.hashStringAlgorithm = hashStringAlgorithm;
    }

}
