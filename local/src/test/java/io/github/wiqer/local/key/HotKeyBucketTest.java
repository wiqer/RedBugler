package io.github.wiqer.local.key;

import io.github.wiqer.local.hash.group.HashFactory;
import io.github.wiqer.local.counter.HotKeyBucket;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@Slf4j
public class HotKeyBucketTest {
    @Test
    public void hotKeyRuleBucket( ) {
         hotKeyRuleBucket(16,6);
    }
    public void hotKeyRuleBucket(int sizeBit,int rateBit) {
        if(sizeBit > 31 || sizeBit < 0){
            sizeBit = 6;
        }
        if(rateBit > 31 || rateBit < 0){
            rateBit = 4;
        }
        if(rateBit > sizeBit){
            rateBit = sizeBit;
        }
        final int size = 1 << sizeBit;
        final int bit = size - 1;
        HashFactory hashFactory = new HashFactory();

        HotKeyBucket bucket = new HotKeyBucket(hashFactory.getAllAlgorithms());
        // 测试添加一些数据
        //HLL myHyperLogLog = new HLL(16, 5);
        Random rand = new Random();

        ArrayList<String> keyList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = Integer.toString(rand.nextInt());
            keyList.add(key);
        }
        // 指数分布参数
        double rate = 1 << rateBit;
        ExponentialDistribution exponentialDistribution = new ExponentialDistribution(rate);
        Set<String> hotKeySet = new HashSet<>();
        // 随机选择 10 次元素（次数可按需调整）
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            // 生成指数分布的随机数
            int randomValue = (int) exponentialDistribution.sample();

            // 将随机数映射到数组索引范围
            int index = randomValue & bit;
            String key = keyList.get(index);
            // 输出随机选择的元素
            if(bucket.getAndSet(key)){
                hotKeySet.add(key);
            }

        }
        long end = System.currentTimeMillis();
        log.info("keyList: " + keyList.size());
        log.info("hotKeySet: " + hotKeySet.size());
        log.info("一百万此检测耗时: " + (end - start) + "ms");
    }

    // 加权随机选择
    public static String weightedRandomChoice(String[] options, int[] weights) {
        int totalWeight = 0;
        for (int weight : weights) {
            totalWeight += weight;
        }
        Random random = new Random();
        int r = random.nextInt(totalWeight);
        int upto = 0;
        for (int i = 0; i < options.length; i++) {
            if (upto + weights[i] > r) {
                return options[i];
            }
            upto += weights[i];
        }
        return null;
    }
    @Test
    public  void gaussianRandom() {
        // 加权随机选择示例
        String[] options = {"A", "B", "C"};
        int[] weights = {1, 2, 3};
        String weightedResult = weightedRandomChoice(options, weights);
        System.out.println("加权随机选择结果: " + weightedResult);

        // 高斯分布随机数示例
        Random gaussianRandom = new Random();
        double mean = 0;
        double stdDev = 1;
        double gaussianResult = mean + stdDev * gaussianRandom.nextGaussian();
        System.out.println("高斯分布随机数结果: " + gaussianResult);

        // 指数分布随机数示例
        double rate = 0.5;
        ExponentialDistribution exponentialDistribution = new ExponentialDistribution(rate);
        double exponentialResult = exponentialDistribution.sample();
        System.out.println("指数分布随机数结果: " + exponentialResult);

        // 泊松分布随机数示例
        int lambdaVal = 2;
        PoissonDistribution poissonDistribution = new PoissonDistribution(lambdaVal);
        int poissonResult = poissonDistribution.sample();
        System.out.println("泊松分布随机数结果: " + poissonResult);
    }
    @Test
    public  void exponentialDistribution( ) {
        // 初始化一个包含 100 个元素的数组
        int[] array = new int[100];
        for (int i = 0; i < 100; i++) {
            array[i] = i;
        }

        // 指数分布参数
        double rate = 50;
        ExponentialDistribution exponentialDistribution = new ExponentialDistribution(rate);

        // 随机选择 10 次元素（次数可按需调整）
        for (int i = 0; i < 100; i++) {
            // 生成指数分布的随机数
            double randomValue = exponentialDistribution.sample();

            // 将随机数映射到数组索引范围
            int index = (int) (randomValue % array.length);

            // 输出随机选择的元素
            System.out.println("第 " + (i + 1) + " 次随机选择的元素: " + array[index]);
        }
    }
}
