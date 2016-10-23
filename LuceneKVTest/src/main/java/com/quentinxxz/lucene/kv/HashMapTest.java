package com.quentinxxz.lucene.kv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * 类HashMapTest.java的实现描述：HashMapKV test
 * 
 * @author quentinxxz 2016年10月23日 下午3:09:21
 */
public class HashMapTest {

    public static void main(String args[]) {
        Map<String, Integer> hashMap = new HashMap<>();
        List<String> keys = new ArrayList<String>();

        AtomicInteger index = new AtomicInteger(0);

        // 生成并插入200w条数据
        Stream.generate(new Supplier<String>() {

            @Override
            public String get() {
                return HashMapTest.getRandomString(20);
            }

        }).limit(2000000).forEach(key -> {
            keys.add(key);
            hashMap.put(key, index.get());

            if (index.get() % 100000 == 0) System.out.println(index + " records has benn inserted");
            index.incrementAndGet();

        });

        // 性能测试，100w次查询用时
        long start;
        for (int i = 0; i < 10; i++) {
            start = System.currentTimeMillis();
            keys.stream().limit(1000000).forEachOrdered(key -> {
                Integer result = hashMap.get(key);
                if (result == null) {
                    System.out.println("not found");
                }

            });
            System.out.println("useed time : " + (System.currentTimeMillis() - start) / 1000.0f + " seconds");
        }
    }

    public static String getRandomString(int length) { // length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

}
