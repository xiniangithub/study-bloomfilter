package com.dscomm.wez.bloomfilter.redis.bitmap;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.nio.charset.Charset;

/**
 * 基于Redis的bitmap实现BloomFilter
 * <br/>参考：https://www.jianshu.com/p/c2defe549b40
 *
 * @Author wez
 * @Date 2021/9/12
 */
public class RedisBitmapBloomFilter {

    private static final String BF_KEY_PREFIX = "bf:";

    private int numApproxElements;
    private double fpp;
    private int numHashFunctions;
    private int bitmapLength;

    private Jedis jedis;

    /**
     * 构造布隆过滤器。注意：在同一业务场景下，三个参数务必相同
     * @param numApproxElements 预估元素数量
     * @param fpp               可接受的最大误差（假阳性率）
     */
    public RedisBitmapBloomFilter(int numApproxElements, double fpp, Jedis jedis) {
        this.numApproxElements = numApproxElements;
        this.fpp = fpp;
        this.jedis = jedis;

        // 计算bitmap长度
        bitmapLength = (int) (-numApproxElements * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        // 计算hash函数个数
        numHashFunctions = Math.max(1, (int) Math.round((double) bitmapLength / numApproxElements * Math.log(2)));
    }

    /**
     * 取得自动计算的最优哈希函数个数
     */
    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    /**
     * 取得自动计算的最优Bitmap长度
     */
    public int getBitmapLength() {
        return bitmapLength;
    }

    /**
     * 计算一个元素值哈希后映射到Bitmap的哪些bit上
     * @param element 元素值
     * @return bit下标的数组
     */
    private long[] getBitIndices(String element) {
        long[] indices = new long[numHashFunctions];

        byte[] bytes = Hashing.murmur3_128()
                .hashObject(element, Funnels.stringFunnel(Charset.forName("UTF-8")))
                .asBytes();

        long hash1 = Longs.fromBytes(
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]
        );
        long hash2 = Longs.fromBytes(
                bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]
        );

        long combinedHash = hash1;
        for (int i = 0; i < numHashFunctions; i++) {
            indices[i] = (combinedHash & Long.MAX_VALUE) % bitmapLength;
            combinedHash += hash2;
        }

        return indices;
    }

    /**
     * 插入元素
     * @param key       原始Redis键，会自动加上'bf:'前缀
     * @param element   元素值，字符串类型
     * @param expireSec 过期时间（秒）
     */
    public void insert(String key, String element, int expireSec) {
        if (key == null || element == null) {
            throw new IllegalArgumentException("Redis key is required.");
        }
        String actualKey = BF_KEY_PREFIX.concat(key);

        Pipeline pipeline = jedis.pipelined();
        for (long index : getBitIndices(element)) {
            pipeline.setbit(actualKey, index, true);
        }
        pipeline.syncAndReturnAll();

        jedis.expire(actualKey, expireSec);
    }

    /**
     * 检查元素在集合中是否（可能）存在
     * @param key     原始Redis键，会自动加上'bf:'前缀
     * @param element 元素值，字符串类型
     */
    public boolean mayExist(String key, String element) {
        if (key == null || element == null) {
            throw new IllegalArgumentException("Redis key is required.");
        }
        String actualKey = BF_KEY_PREFIX.concat(key);
        boolean result = false;

        Pipeline pipeline = jedis.pipelined();
        for (long index : getBitIndices(element)) {
            pipeline.getbit(actualKey, index);
        }
        result = !pipeline.syncAndReturnAll().contains(false);

        return result;
    }

    public static void main(String[] args) {
        final int numApproxElements = 3000;
        final double fpp = 0.03;
        final int daySec = 60 * 60 * 24;

        Jedis jedis = new Jedis("127.0.0.1", 6379);
        if (!"PONG".equals(jedis.ping())) {
            System.out.println("连接Redis失败");
            return;
        }

        try {
            RedisBitmapBloomFilter redisBloomFilter = new RedisBitmapBloomFilter(numApproxElements, fpp, jedis);
            System.out.println("numHashFunctions: " + redisBloomFilter.getNumHashFunctions());
            System.out.println("bitmapLength: " + redisBloomFilter.getBitmapLength());

            redisBloomFilter.insert("topic_read:8839540:20190609", "76930242", daySec);
            redisBloomFilter.insert("topic_read:8839540:20190609", "76930243", daySec);
            redisBloomFilter.insert("topic_read:8839540:20190609", "76930244", daySec);
            redisBloomFilter.insert("topic_read:8839540:20190609", "76930245", daySec);
            redisBloomFilter.insert("topic_read:8839540:20190609", "76930246", daySec);

            System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930242"));
            System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930244"));
            System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930246"));
            System.out.println(redisBloomFilter.mayExist("topic_read:8839540:20190609", "76930248"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }

}
