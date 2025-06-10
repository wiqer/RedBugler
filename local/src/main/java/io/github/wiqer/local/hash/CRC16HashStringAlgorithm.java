package io.github.wiqer.local.hash;

/**
 * CRC16哈希算法实现
 * 特点：
 * 1. 使用CRC16算法
 * 2. 性能一般
 * 3. 分布均匀
 */
public class CRC16HashStringAlgorithm implements HashStringAlgorithm {
    private static final int[] CRC16_TABLE = new int[256];
    
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++) {
                crc = ((crc & 1) == 1) ? ((crc >>> 1) ^ 0xA001) : (crc >>> 1);
            }
            CRC16_TABLE[i] = crc;
        }
    }

    @Override
    public int getHash(Object key) {
        if (key == null) {
            return 0;
        }
        
        String str = key.toString();
        byte[] data = str.getBytes();
        int crc = 0xFFFF;
        
        for (byte b : data) {
            crc = (crc >>> 8) ^ CRC16_TABLE[(crc ^ b) & 0xFF];
        }
        
        return crc & 0xFFFF;
    }
}