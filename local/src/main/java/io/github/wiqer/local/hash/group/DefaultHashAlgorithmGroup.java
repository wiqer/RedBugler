package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.*;

public class DefaultHashAlgorithmGroup implements HashAlgorithmGroup {

    AbcHashStringAlgorithm abcHashAlgorithm = new AbcHashStringAlgorithm();

    CRC16HashStringAlgorithm crc16HashAlgorithm = new CRC16HashStringAlgorithm();

    JavaHashStringAlgorithm javaHashAlgorithm = new JavaHashStringAlgorithm();

    Bit64HashStringAlgorithm bit64HashAlgorithm = new Bit64HashStringAlgorithm();

    Java71HashStringAlgorithm java71HashAlgorithm = new Java71HashStringAlgorithm();

    RandomHashStringAlgorithm randomHashAlgorithm = new RandomHashStringAlgorithm();

    SimpleHashStringAlgorithm simpleHashAlgorithm = new SimpleHashStringAlgorithm();

    @Override
    public HashStringAlgorithm getByName(String name){
        if("abc".equalsIgnoreCase(name)){
            return abcHashAlgorithm;
        }
        else if("crc16".equalsIgnoreCase(name)){
            return crc16HashAlgorithm;
        }
        else if("java".equalsIgnoreCase(name)){
            return javaHashAlgorithm;
        }
        else if("bit64".equalsIgnoreCase(name)){
            return bit64HashAlgorithm;
        }
        else if("java71".equalsIgnoreCase(name)){
            return java71HashAlgorithm;
        }
        else if("random".equalsIgnoreCase(name)){
            return randomHashAlgorithm;
        }
        else if("simple".equalsIgnoreCase(name)){
            return simpleHashAlgorithm;
        }
        else {
            return null;
        }
    }

}
