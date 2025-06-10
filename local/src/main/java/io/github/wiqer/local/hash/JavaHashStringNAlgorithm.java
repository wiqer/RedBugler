package io.github.wiqer.local.hash;

/**
 * @author Administrator
 */
public class JavaHashStringNAlgorithm implements HashStringAlgorithm {
    final int moveHashBits ;

    public JavaHashStringNAlgorithm(int moveHashBits) {
        if (moveHashBits < 1 || moveHashBits > 32){
            throw new IllegalArgumentException("moveHashBits must be between 1 and 32");
        }
        this.moveHashBits = moveHashBits;
    }

    @Override
    public int getHash(Object key) {
        int h = key.hashCode();
        h = (h >>> moveHashBits) | (h << (32- moveHashBits)) ;
        return (h ) ^ (h >>> 16);
    }

    @Override
    public boolean isFast(){
        return true;
    }
}
