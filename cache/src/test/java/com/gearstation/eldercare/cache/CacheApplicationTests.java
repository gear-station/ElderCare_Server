package com.gearstation.eldercare.cache;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CacheApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Autowired
    JedisPool jedisPool;
    @Test
    public void redisTest(){


        String uuid = UUID.randomUUID().toString();
        Jedis jedis = jedisPool.getResource();
        jedis.setex("ssssssss", 1000, uuid);

    }

}
