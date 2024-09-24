package io.github.wiqer.local.key;

import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Random;

public class KeyIntFragmentTest {

    @Test
    public void randomHLL() throws FileNotFoundException {
        HLL myHyperLogLog = new HLL(16,5,-1, true, HLLType.EMPTY);
        // 测试添加一些数据
        //HLL myHyperLogLog = new HLL(16, 5);
        Random rand = new Random();

        for (int i = 0; i < 100000; i++) {
            String key = Integer.toString(rand.nextInt()) + System.currentTimeMillis();
            myHyperLogLog.addRaw(key.hashCode());
        }
        System.out.println("Estimated number of unique elements: " + myHyperLogLog.cardinality());

    }

    @Test
    public void HLLOder()  {
        HLL myHyperLogLog = new HLL((17 >>2) ,5,-1, true, HLLType.EMPTY);
        // 测试添加一些数据
        //HLL myHyperLogLog = new HLL(16, 5);
        Random rand = new Random();
        int sum = 10000000;
        System.out.println("  number of getIntBitsNumber: " +getIntBitsNumber(sum));

        for (int i = 0; i < sum; i++) {
            myHyperLogLog.addRaw(i);
        }
        System.out.println("Estimated number of unique elements: " + myHyperLogLog.cardinality());
        System.out.println("Estimated number of unique elements percentage: " + ((double)myHyperLogLog.cardinality())/(double)sum);

    }

    private static int getIntBitsNumber(int number) {
        int bitsnumber = 0;
        number = number & Integer.MAX_VALUE;
        while (number > 0){
            bitsnumber++;
            number = number >> 1;
        }
        return bitsnumber;
    }

    @Test
    public void randomHLLOderHalf()  {
        double augSum = 0;
        for (int sum = 1000; sum < 10000000; sum*=10) {
            HLL myHyperLogLog = new HLL((17 >>2)  ,5,-1, true, HLLType.EMPTY);
            // 测试添加一些数据
            //HLL myHyperLogLog = new HLL(16, 5);
            Random rand = new Random();

            System.out.println("  number of getIntBitsNumber: " +getIntBitsNumber(sum));

            for (int i = 0; i < sum; i++) {
                String key = Integer.toString(rand.nextInt()) + System.currentTimeMillis() + i;
                myHyperLogLog.addRaw(key.hashCode());
            }
            System.out.println("sum" + sum+ "Estimated number of unique elements: " + myHyperLogLog.cardinality());
            double aug = ((double)myHyperLogLog.cardinality())/(double)sum;
            System.out.println("sum" + sum+ "Estimated number of unique elements percentage: " + aug);
            augSum += aug;
        }
        System.out.println("aug = " +  augSum /4);
    }

    @Test
    public void randomHLLOderHalfSubtractOne()  {
        double augSum = 0;
        for (int sum = 1000; sum < 10000000; sum*=10) {
            HLL myHyperLogLog = new HLL(Math.max((17 >>2) - 1 , 4) ,5,-1, true, HLLType.EMPTY);
            // 测试添加一些数据
            //HLL myHyperLogLog = new HLL(16, 5);
            Random rand = new Random();

            System.out.println("  number of getIntBitsNumber: " +getIntBitsNumber(sum));

            for (int i = 0; i < sum; i++) {
                String key = Integer.toString(rand.nextInt()) + System.currentTimeMillis() + i;
                myHyperLogLog.addRaw(key.hashCode());
            }
            System.out.println("sum" + sum+ "Estimated number of unique elements: " + myHyperLogLog.cardinality());
            double aug = ((double)myHyperLogLog.cardinality())/(double)sum;
            System.out.println("sum" + sum+ "Estimated number of unique elements percentage: " + aug);
            augSum += aug;
        }
        System.out.println("aug = " +  augSum /4);
    }
    @Test
    public void randomHLLOderHalfAndOne()  {
        double augSum = 0;
        for (int sum = 1000; sum < 10000000; sum*=10) {
            HLL myHyperLogLog = new HLL(Math.max((17 >>2) + 1 , 4) ,5,-1, true, HLLType.EMPTY);
            // 测试添加一些数据
            //HLL myHyperLogLog = new HLL(16, 5);
            Random rand = new Random();

            System.out.println("  number of getIntBitsNumber: " +getIntBitsNumber(sum));

            for (int i = 0; i < sum; i++) {
                String key = Integer.toString(rand.nextInt()) + System.currentTimeMillis() + i;
                myHyperLogLog.addRaw(key.hashCode());
            }
            System.out.println("sum" + sum+ "Estimated number of unique elements: " + myHyperLogLog.cardinality());
            double aug = ((double)myHyperLogLog.cardinality())/(double)sum;
            System.out.println("sum" + sum+ "Estimated number of unique elements percentage: " + aug);
            augSum += aug;
        }
        System.out.println("aug = " +  augSum /4);
    }

    public double randomHLLOderHalfAndOne(int and)  {
        double augSum = 0;
        for (int sum = 1000; sum < 10000000; sum*=10) {
            HLL myHyperLogLog = new HLL(Math.max((getIntBitsNumber(sum) >>2) + and , 4) ,5,-1, true, HLLType.EMPTY);
            // 测试添加一些数据
            //HLL myHyperLogLog = new HLL(16, 5);
            Random rand = new Random();

            //System.out.println("  number of getIntBitsNumber: " +getIntBitsNumber(sum));

            for (int i = 0; i < sum; i++) {
                String key = Integer.toString(rand.nextInt()) + System.currentTimeMillis() + i;
                myHyperLogLog.addRaw(key.hashCode());
            }
            //System.out.println("sum" + sum+ "Estimated number of unique elements: " + myHyperLogLog.cardinality());
            double aug = ((double)myHyperLogLog.cardinality())/(double)sum;
            //System.out.println("sum" + sum+ "Estimated number of unique elements percentage: " + aug);
            augSum += aug;
        }
        //System.out.println("aug = " +  augSum /4);
        return  augSum /4;
    }
    @Test
    public void hllPerformanceTest()  {
        double augSum = 0;
        int times = 1000;

        for (int j = -4 ; j < 5; j++) {
            for (int i = 0; i < times; i++) {
                augSum += randomHLLOderHalfAndOne(j);
            }
            System.out.println("AndOne = "+ j + "aug = " +  augSum /times);
            augSum = 0;
        }
    }


}
