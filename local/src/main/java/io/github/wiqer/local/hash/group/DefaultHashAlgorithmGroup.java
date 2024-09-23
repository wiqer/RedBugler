package io.github.wiqer.local.hash.group;

import io.github.wiqer.local.hash.*;

public class DefaultHashAlgorithmGroup implements HashAlgorithmGroup {

    AbcHashAlgorithm abcHashAlgorithm = new AbcHashAlgorithm();

    CRC16HashAlgorithm crc16HashAlgorithm = new CRC16HashAlgorithm();

    JavaHashAlgorithm javaHashAlgorithm = new JavaHashAlgorithm();

    Bit64HashAlgorithm bit64HashAlgorithm = new Bit64HashAlgorithm();

    Java71HashAlgorithm java71HashAlgorithm = new Java71HashAlgorithm();

    RandomHashAlgorithm randomHashAlgorithm = new RandomHashAlgorithm();

    SimpleHashAlgorithm simpleHashAlgorithm = new SimpleHashAlgorithm();

    HashAlgorithm getByName(String name){
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
