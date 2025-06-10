package io.github.wiqer.local.hash;

/**
 * MurmurHash3 32位哈希算法实现
 * 特点：
 * 1. 高性能
 * 2. 低碰撞率
 * 3. 分布均匀
 */
public class MurmurHash3 implements HashStringAlgorithm {
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int R1 = 15;
    private static final int R2 = 13;
    private static final int M = 5;
    private static final int N = 0xe6546b64;

    @Override
    public int getHash(Object key) {
        if (key == null) {
            return 0;
        }
        
        String str = key.toString();
        byte[] data = str.getBytes();
        int length = data.length;
        int h1 = 0x9747b28c;
        int roundedEnd = length & 0xfffffffc;
        
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | 
                    ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, R1);
            k1 *= C2;
            
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, R2);
            h1 = h1 * M + N;
        }
        
        int k1 = 0;
        switch (length & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
        }
        
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        
        return h1;
    }
} 