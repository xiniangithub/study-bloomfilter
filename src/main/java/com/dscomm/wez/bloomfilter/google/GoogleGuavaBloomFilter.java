package com.dscomm.wez.bloomfilter.google;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 使用Google guava中的BloomFilter
 */
public class GoogleGuavaBloomFilter {

    public static void main(String[] args) {
        /*
         * BloomFilter通过create()静态方法创建对象实例
         * create()方法参数说明
         *      funnel: BloomFilter中存放数据的数据类型
         *      expectedInsertions：存放数据的个数
         *      fpp：误判率
         */
        int size = 100_0000;
        int oneTenthOfSize = size / 10;
        BloomFilter bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), size, 0.001);

        Random random = new Random();

        // 存放BloomFilter中存在十分之一的元素
        List<String> list1 = new ArrayList<>(oneTenthOfSize);
        for (int i=0; i<size; i++) {
            String uuid = UUID.randomUUID().toString()+random.nextInt(size);
            bloomFilter.put(uuid);

            if (i < oneTenthOfSize) {
                list1.add(uuid);
            }
        }

        // 存放BloomFilter中不存在五分之一的元素
        int oneFifthOfSize = size / 5;
        List<String> list2 = new ArrayList<>(oneFifthOfSize);
        for (int i=0; i<oneFifthOfSize; i++) {
            list2.add(UUID.randomUUID().toString()+random.nextInt(size));
        }

        // 对list1中的元素进行判断是否在BloomFilter中
        int exitsNum = 0;
        for (int i=0; i<list1.size(); i++) {
            final boolean exits = bloomFilter.mightContain(list1.get(i));
            if (exits) {
                exitsNum++;
            }
        }
        System.out.println("在<已存在>布隆过滤器的"+size+"条数据中，布隆过滤器判断存在"+exitsNum+"条数据");

        // 对list2中的元素进行判断是否在BloomFilter中
        int exitsNum2 = 0;
        for (int i=0; i<list2.size(); i++) {
            final boolean exits = bloomFilter.mightContain(list2.get(i));
            if (exits) {
                exitsNum2++;
            }
        }
        System.out.println("在<不存在>布隆过滤器的"+size+"条数据中，布隆过滤器判断存在"+exitsNum2+"条数据");

        // 命中率
        NumberFormat percentFormat = NumberFormat.getPercentInstance();
        percentFormat.setMaximumFractionDigits(2);
        float percent = (float) exitsNum / size;
        float bingo = (float) exitsNum2 / size;
        System.out.println("命中率为：" + percentFormat.format(percent) + "，误判率为：" + percentFormat.format(bingo));
    }

}
