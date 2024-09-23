package io.github.wiqer.local.key;

import java.util.BitSet;

public class HyperLogLog {
    private static final int BITS = 30;
    private static final int SIZE = 1 << BITS;
    private final BitSet bitSet;

    public HyperLogLog() {
        bitSet = new BitSet(SIZE);
    }

    public void add(int hash) {
        int position = hash & (SIZE - 1);
        int rank = 0;
        for (int i = 0; i < BITS; i++) {
            if ((hash & (1L << (BITS - 1 - i))) == 0) {
                rank++;
            } else {
                // 检查当前位置的 rank 是否小于我们计算的 rank
                if (rank > getRank(position, i)) {
                    setRank(position, i, rank);
                }
                break; // 一旦设置了位，就跳出循环
            }
        }
    }

    private int getRank(int position, int i) {
        // 计算当前位置的 rank
        int rank = 0;
        for (int j = i; j < BITS; j++) {
            if (bitSet.get(position + (1 << j))) {
                rank++;
            } else {
                break;
            }
        }
        return rank;
    }

    private void setRank(int position, int i, int rank) {
        // 设置当前位置的 rank
        for (int j = i; j < BITS; j++) {
            if (rank > 0) {
                bitSet.set(position + (1 << j), true);
                rank--;
            } else {
                break;
            }
        }
    }

    public double estimate() {
        long sum = 0;
        for (int i = 0; i < SIZE; i++) {
            if (getRank(i, 0) > 0) {
                sum += 1L << (BITS - getRank(i, 0));
            }
        }
        return (double) SIZE * Math.log(SIZE * 1.0 / sum) / Math.log(2);
    }

    public static void main(String[] args) {
        HyperLogLog hll = new HyperLogLog();
        // 测试添加一些数据
        for (int i = 0; i < 10000; i++) {
            hll.add(i);
        }

        System.out.println("Estimated number of unique elements: " + hll.estimate());
    }
}
