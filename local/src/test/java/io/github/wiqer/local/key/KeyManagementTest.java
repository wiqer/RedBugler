package io.github.wiqer.local.key;

import io.github.wiqer.local.counter.HotKeyBucket;
import io.github.wiqer.local.counter.KeyManagement;
import io.github.wiqer.local.hash.group.HashFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class KeyManagementTest {

    @Test
    public void KeyManagement() throws ExecutionException, InterruptedException {
        KeyManagement(20, 6);
    }

    public void KeyManagement(int sizeBit, int rateBit) throws ExecutionException, InterruptedException {
        if (sizeBit > 31 || sizeBit < 0) {
            sizeBit = 6;
        }
        if (rateBit > 31 || rateBit < 0) {
            rateBit = 4;
        }
        if (rateBit > sizeBit) {
            rateBit = sizeBit;
        }
        final int size = 1 << sizeBit;
        final int bit = size - 1;


        KeyManagement management = new KeyManagement(2, 30, TimeUnit.MILLISECONDS);
        // 测试添加一些数据
        //HLL myHyperLogLog = new HLL(16, 5);
        Random rand = new Random();
        Map<String, Integer> keyCountMap = new HashMap<>();

        ArrayList<String> keyList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = Integer.toString(rand.nextInt());
            keyList.add(key);
        }

        // 指数分布参数
        double rate = 1 << rateBit;
        log.info("Key management rate: {}", rate);
        ExponentialDistribution exponentialDistribution = new ExponentialDistribution(rate);
        ArrayList<String> keySet = new ArrayList<>(size);
        for (int i = 0; i < 1000000; i++) {
            // 生成指数分布的随机数
            int randomValue = (int) exponentialDistribution.sample();

            // 将随机数映射到数组索引范围
            int index = randomValue & bit;
            String key = keyList.get(index);
            keySet.add(key);
            keyCountMap.compute(key, (k, v) -> v == null ? 1 : v + 1);

        }
        HashMap map = new HashMap(keyCountMap);
        log.info("全部探测的key数量：" + keyCountMap.size());
        log.info("key max time ：" + keyCountMap.values().stream().max(Integer::compareTo).get());
        Set<String> hotKeySet = new HashSet<>();
        // 随机选择 10 次元素（次数可按需调整）
        long start = System.currentTimeMillis();
        for (String key : keySet) {
            // 输出随机选择的元素
            if (management.getAndSet(key)) {
                hotKeySet.add(key);
                if (keyCountMap.get(key) == null) {
                    log.warn(key + " ,这个键在初期算热，但是整体非热键,对应次数：" + map.get(key));
                }
            }

        }
        long end = System.currentTimeMillis();

        log.info("keyList: " + keyList.size());
        log.info("hotKeySet: " + hotKeySet.size());
        log.info("一百万此检测耗时: " + (end - start) + "ms");
    }

    @Test
    public void KeyManagementGet() throws InterruptedException {
        KeyManagementGet(20, 6);
    }

    public void KeyManagementGet(int sizeBit, int rateBit) throws InterruptedException {
        if (sizeBit > 31 || sizeBit < 0) {
            sizeBit = 6;
        }
        if (rateBit > 31 || rateBit < 0) {
            rateBit = 4;
        }
        if (rateBit > sizeBit) {
            rateBit = sizeBit;
        }
        final int size = 1 << sizeBit;
        final int bit = size - 1;


        KeyManagement management = new KeyManagement(4, 30, TimeUnit.MILLISECONDS);
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
        ArrayList<String> heySet = new ArrayList<>(size);
        for (int i1 = 0; i1 < 10000; i1++) {
            // 生成指数分布的随机数
            int randomValue = (int) exponentialDistribution.sample();

            // 将随机数映射到数组索引范围
            int index = randomValue & bit;
            String key = keyList.get(index);
            // 输出随机选择的元素
            heySet.add(key);
        }
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 30; i++) {
            Thread.sleep(30);
            threadPoolExecutor.submit(() -> {

                Set<String> hotKeySet = new HashSet<>();
                // 随机选择 10 次元素（次数可按需调整）
                long start = System.currentTimeMillis();
                for (String key : heySet) {
                    // 输出随机选择的元素
                    try {
                        if (management.get(key, 20, TimeUnit.MILLISECONDS)) {
                            hotKeySet.add(key);
                        }
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
                long end = System.currentTimeMillis();
                log.info("keyList: " + keyList.size());
                log.info("hotKeySet: " + hotKeySet.size());
                log.info("一万此检测耗时: " + (end - start) + "ms");
            });
        }
        threadPoolExecutor.awaitTermination(1, TimeUnit.MINUTES);

    }

    @Test
    public void KeyManagementGetAndSet() throws InterruptedException {
        KeyManagementGetAndSet(20, 6);
    }

    public void KeyManagementGetAndSet(int sizeBit, int rateBit) throws InterruptedException {
        if (sizeBit > 31 || sizeBit < 0) {
            sizeBit = 6;
        }
        if (rateBit > 31 || rateBit < 0) {
            rateBit = 4;
        }
        if (rateBit > sizeBit) {
            rateBit = sizeBit;
        }
        final int size = 1 << sizeBit;
        final int bit = size - 1;


        KeyManagement management = new KeyManagement(8, 30, TimeUnit.MILLISECONDS);
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
        ArrayList<String> heySet = new ArrayList<>(size);
        for (int i1 = 0; i1 < 10000; i1++) {
            // 生成指数分布的随机数
            int randomValue = (int) exponentialDistribution.sample();

            // 将随机数映射到数组索引范围
            int index = randomValue & bit;
            String key = keyList.get(index);
            // 输出随机选择的元素
            heySet.add(key);
        }
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 30; i++) {
            Thread.sleep(30);
            threadPoolExecutor.submit(() -> {

                Set<String> hotKeySet = new HashSet<>();
                // 随机选择 10 次元素（次数可按需调整）
                long start = System.currentTimeMillis();
                for (String key : heySet) {
                    // 输出随机选择的元素
                    if (management.getAndSet(key)) {
                        hotKeySet.add(key);
                    }
                }
                long end = System.currentTimeMillis();
                log.info("keyList: " + keyList.size());
                log.info("hotKeySet: " + hotKeySet.size());
                log.info("一万此检测耗时: " + (end - start) + "ms");
            });
        }
        threadPoolExecutor.awaitTermination(1, TimeUnit.MINUTES);

    }

    @Test
    public void KeyManagementGetAndSetPre() throws InterruptedException {
        KeyManagementGetAndSetPre(20, 6);
    }

    public void KeyManagementGetAndSetPre(int sizeBit, int rateBit) throws InterruptedException {
        if (sizeBit > 31 || sizeBit < 0) {
            sizeBit = 6;
        }
        if (rateBit > 31 || rateBit < 0) {
            rateBit = 4;
        }
        if (rateBit > sizeBit) {
            rateBit = sizeBit;
        }
        final int size = 1 << sizeBit;
        final int bit = size - 1;


        KeyManagement management = new KeyManagement(8, 30, TimeUnit.MILLISECONDS);
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
        ArrayList<String> heySet = new ArrayList<>(size);
        for (int i1 = 0; i1 < 10000; i1++) {
            // 生成指数分布的随机数
            int randomValue = (int) exponentialDistribution.sample();

            // 将随机数映射到数组索引范围
            int index = randomValue & bit;
            String key = keyList.get(index);
            // 输出随机选择的元素
            heySet.add(key);
        }
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 30; i++) {
            Thread.sleep(30);
            threadPoolExecutor.submit(() -> {

                Set<String> hotKeySet = new HashSet<>();
                // 随机选择 10 次元素（次数可按需调整）
                long start = System.currentTimeMillis();
                for (String key : heySet) {
                    // 输出随机选择的元素
                    try {
                        if (management.get(key, (sum, allTimes, groupCount) -> sum > (allTimes / groupCount) >>> 2, 20, TimeUnit.MILLISECONDS)) {
                            hotKeySet.add(key);
                        }
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
                long end = System.currentTimeMillis();
                log.info("keyList: " + keyList.size());
                log.info("hotKeySet: " + hotKeySet.size());
                log.info("一万此检测耗时: " + (end - start) + "ms");
            });
        }
        threadPoolExecutor.awaitTermination(1, TimeUnit.MINUTES);

    }
}
