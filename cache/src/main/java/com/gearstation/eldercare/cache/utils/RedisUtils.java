package com.gearstation.eldercare.cache.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.SortingParams;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Description: Redis tools <br>
 * Copyright © 2019 www.gear-station.com <br>
 * CreateTime: 2019/05/12 23:01 <br>
 *
 * @author packy <br>
 * @version 1.0.1 <br>
 */
@Component
@Log4j2
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RedisUtils {

    private final JedisPool jedisPool;

    /**
     * Description: Retrieve value by key from specified DB, and release the connection <br>
     * CreateTime 2019-05-12 23:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return value of input key if success, or null if fail <br>
     * @author packy <br>
     */
    public String get(final String key, final int dbIndex) {
        Jedis jedis = null;
        String value = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            value = jedis.get(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return value;
    }

    /**
     * Description: Add value to specified DB, and release the connection <br>
     * CreateTime 2019-05-14 23:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success, or NG if fail <br>
     * @author packy <br>
     */
    public String set(final String key, final String value, final int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.set(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
            return "NG";
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Remove key and value from specified DB <br>
     * CreateTime 2019-05-14 23:45 <br>
     *
     * @param keys    <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success, or NG if fail <br>
     * @author packy <br>
     */
    public Long remove(final int dbIndex, final String... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.del(keys);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Append value by key from specified DB <br>
     * CreateTime 2019-05-14 23:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return length of value if success, or 0L if fail <br>
     * @author packy <br>
     */
    public Long append(final String key, final String value, final int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.append(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Check if key is existing <br>
     * CreateTime 2019-05-14 23:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return length of value if success, or 0L if fail <br>
     * @author packy <br>
     */
    public Boolean isExisting(final String key, final int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.exists(key);
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Clear all keys in current DB <br>
     * CreateTime 2019-05-14 23:45 <br>
     *
     * @return Always return OK <br>
     * @author packy <br>
     */
    public String flushDB() {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.flushDB();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return null;
    }

    /**
     * Description: Give expire time to key from specified DB, and release the connection <br>
     * CreateTime 2019-05-12 23:45 <br>
     *
     * @param key     <br>
     * @param time    expire time, unit is second <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return 1 if success, or 0 if fail <br>
     * @author packy <br>
     */
    public Long expire(String key, int time, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.expire(key, time);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Return expire time for key from specified DB, and release the connection <br>
     * CreateTime 2019-05-16 18:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return -2 if key doesn't exist, -1 if key exists but has no expire time, 0 when exception occurs. Otherwise, return expire time with sec unit <br>
     * @author packy <br>
     */
    public Long ttl(String key, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.ttl(key);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Persist a specified key, and release the connection <br>
     * CreateTime 2019-05-20 16:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return 1 if success, 0 if key doesn't exist or has no expire time, -1 when exception occurs. <br>
     * @author packy <br>
     */
    public Long persist(String key, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.persist(key);
        } catch (Exception e) {
            log.error(e.getMessage());
            return -1L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Add a key with expire time. If key exists, will be overwritten <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @param seconds Expire time, unit: second
     * @return Return OK if success, or null if fail. <br>
     * @author packy <br>
     */
    public String setex(String key, String value, int seconds, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.setex(key, seconds, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return null;
    }

    /**
     * Description: Add a new key, if it exists, do nothing <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return 1 if success, or 0 if key exists or exception. <br>
     * @author packy <br>
     */
    public Long setnx(String key, String value, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.setnx(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Set a new value to specified key and return old value <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return old value, or nil if key doesn't exist <br>
     * @author packy <br>
     */
    public String getSet(String key, String value, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.getSet(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return null;
    }

    /**
     * Description: Replace string value from offset position for specified key <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key     <br>
     * @param str     <br>
     * @param offset  Start index<br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return length of string after replacement, or 0 if fail <br>
     * @author packy <br>
     */
    public Long setRange(String key, String str, int offset, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.setrange(key, offset, str);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }
    }

    /**
     * Description: Get values by keys <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param keys    String array or a single key <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return value list of specified keys <br>
     * @author packy <br>
     */
    public List<String> mget(int dbIndex, String... keys) {
        Jedis jedis = null;
        List<String> values = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            values = jedis.mget(keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return values;
    }

    /**
     * Description: Add new key-values at same time <br>
     * CreateTime 2019-05-20 23:45 <br>
     * Example obj.mset(new String[]{"key2","value1","key2","value2"})
     *
     * @param keysvalues key-values <br>
     * @param dbIndex    DB index from 0 to 15 <br>
     * @return Return OK if success, or null if fail <br>
     * @author packy <br>
     */
    public String mset(int dbIndex, String... keysvalues) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.mset(keysvalues);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Add new key-values at same time if key doesn't exist. When one key is duplicated, whole operation will roll back <br>
     * CreateTime 2019-05-20 23:45 <br>
     * Example obj.msetnx(new String[]{"key2","value1","key2","value2"})
     *
     * @param keysvalues key-values <br>
     * @param dbIndex    DB index from 0 to 15 <br>
     * @return Return 1 if success, or 0 if fail <br>
     * @author packy <br>
     */
    public Long msetnx(int dbIndex, String... keysvalues) {
        Jedis jedis = null;
        Long res = 0L;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.msetnx(keysvalues);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get sub-string value for specified key <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key         <br>
     * @param startOffset Start index, if it is negative, that means start from right <br>
     * @param endOffset   End index<br>
     * @param dbIndex     DB index from 0 to 15 <br>
     * @return Return null if key doesn't exist <br>
     * @author packy <br>
     */
    public String getrange(String key, int startOffset, int endOffset, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Increase value by 1, exception occurs if value isn't int  <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return increased value or 1 if key doesn't exist <br>
     * @author packy <br>
     */
    public Long incr(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.incr(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Increase value by specified value, exception occurs if value isn't int  <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key       <br>
     * @param increment <br>
     * @param dbIndex   DB index from 0 to 15 <br>
     * @return Return increased value or 1 if key doesn't exist <br>
     * @author packy <br>
     */
    public Long incrBy(String key, Long increment, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.incrBy(key, increment);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Decrease value by 1, exception occurs if value isn't int  <br>
     * CreateTime 2019-05-20 23:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return increased value or -1 if key doesn't exist <br>
     * @author packy <br>
     */
    public Long decr(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.decr(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Decrease value by specified step value, exception occurs if value isn't int  <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key       <br>
     * @param decrement <br>
     * @param dbIndex   DB index from 0 to 15 <br>
     * @return Return decreased value or 1 if key doesn't exist <br>
     * @author packy <br>
     */
    public Long decrBy(String key, Long decrement, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.decrBy(key, decrement);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get length of specified key  <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return null if failed <br>
     * @author packy <br>
     */
    public Long serlen(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.strlen(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Set a field-value map for a specified key. If the map doesn't exist, it will be create automatically <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key     <br>
     * @param field   Map key     <br>
     * @param value   Map value     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return 0 if map doesn't exist, 1 if value is overwritten by new value, null if failed <br>
     * @author packy <br>
     */
    public Long hset(String key, String field, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hset(key, field, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Set a field-value map for a specified key. If the map doesn't exist, it will be create automatically. <br>
     * If the value is existing, operation will be cancelled <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key     <br>
     * @param field   Map key     <br>
     * @param value   Map value     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return 0 if value exists, 1 if success <br>
     * @author packy <br>
     */
    public Long hsetnx(String key, String field, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hsetnx(key, field, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Muilt-Set a field-value map for a specified key. If the map doesn't exist, it will be create automatically. <br>
     * If the value is existing, operation will be cancelled <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key     <br>
     * @param hash    <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    public String hmset(String key, Map<String, String> hash, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hmset(key, hash);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get value of field for specified key <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key     <br>
     * @param field   Map key      <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return nil if hashmap or field doesn't exist <br>
     * @author packy <br>
     */
    public String hget(String key, String field, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hget(key, field);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Multi-Get value of field for specified key <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key     <br>
     * @param fields  Map keys      <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return nil if hashmap or field doesn't exist <br>
     * @author packy <br>
     */
    public List<String> hmget(String key, int dbIndex, String... fields) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hmget(key, fields);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Increase value of field by specified increment <br>
     * CreateTime 2019-05-22 15:45 <br>
     *
     * @param key       <br>
     * @param increment <br>
     * @param field     Map keys      <br>
     * @param dbIndex   DB index from 0 to 15 <br>
     * @return Return increased value <br>
     * @author packy <br>
     */
    public Long hincrby(String key, String field, Long increment, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hincrBy(key, field, increment);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Check existence of value by key and field <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param field   Map keys      <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return true if existing <br>
     * @author packy <br>
     */
    public Boolean hexists(String key, String field, int dbIndex) {
        Jedis jedis = null;
        Boolean res = false;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hexists(key, field);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get the number of fields <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of fields <br>
     * @author packy <br>
     */
    public Long hlen(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hlen(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;

    }

    /**
     * Description: Remove field by specified key <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param fields  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return true if success <br>
     * @author packy <br>
     */
    public Long hdel(String key, int dbIndex, String... fields) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hdel(key, fields);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get all fields by specified key <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return set of fields <br>
     * @author packy <br>
     */
    public Set<String> hkeys(String key, int dbIndex) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hkeys(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get all values by specified key <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return set of values <br>
     * @author packy <br>
     */
    public List<String> hvals(String key, int dbIndex) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hvals(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get all field-values mapping by specified key <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return all field-values mapping <br>
     * @author packy <br>
     */
    public Map<String, String> hgetall(String key, int dbIndex) {
        Jedis jedis = null;
        Map<String, String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.hgetAll(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Add elements to left side of list <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @param strs    elements will be added one by one <br>
     * @return Return size of list <br>
     * @author packy <br>
     */
    public Long lpush(String key, int dbIndex, String... strs) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.lpush(key, strs);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Add elements to right side of list <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @param strs    elements will be added one by one <br>
     * @return Return size of list <br>
     * @author packy <br>
     */
    public Long rpush(String key, int dbIndex, String... strs) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.rpush(key, strs);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Add elements before or after specified position of list <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param where   Enum of ListPosition <br>
     * @param pivot   Existing element in list <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @param value   Element will be added<br>
     * @return Return true if success <br>
     * @author packy <br>
     */
    public Long linsert(String key, ListPosition where, String pivot,
                        String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.linsert(key, where, pivot, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Set value to specified index of list <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param index   Index of list <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @param value   Element will be added<br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    public String lset(String key, Long index, String value, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.lset(key, index, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove count specified same value from list<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param count   How many values should be removed <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @param value   Element will be added<br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    public Long lrem(String key, long count, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.lrem(key, count, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Retain values from start to end<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param start   Start index <br>
     * @param end     End index <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    public String ltrim(String key, long start, long end, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.ltrim(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove one value from head of list<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    synchronized public String lpop(String key, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.lpop(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove one value from tail of list<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    synchronized public String rpop(String key, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.rpop(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove one value from head of one list and add it to head of another list<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param srckey  <br>
     * @param dstkey  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    public String rpoplpush(String srckey, String dstkey, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.rpoplpush(srckey, dstkey);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get value corresponding to specified index in list<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param index   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success, and nil if value doesn't exist <br>
     * @author packy <br>
     */
    public String lindex(String key, long index, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.lindex(key, index);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get size of list by specified key<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return size of list if success<br>
     * @author packy <br>
     */
    public Long llen(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.llen(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get values corresponding to specified start and end index in list<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param start   Start index <br>
     * @param end     End index <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return all values <br>
     * @author packy <br>
     */
    public List<String> lrange(String key, long start, long end, int dbIndex) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.lrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get sorted values<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key               <br>
     * @param sortingParameters <br>
     * @param dbIndex           DB index from 0 to 15 <br>
     * @return Return sorted values <br>
     * @author packy <br>
     */
    public List<String> sort(String key, SortingParams sortingParameters, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.sort(key, sortingParameters);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return null;
    }

    /**
     * Description: Get sorted values with default sort method. Values should be numbers, they will be converted to double and sorted then <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return sorted values <br>
     * @author packy <br>
     */
    public List<String> sort(String key, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.sort(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return null;
    }

    /**
     * Description: Add multiple values to a set by specified key<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param values  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of values added successfully <br>
     * @author packy <br>
     */
    public Long sadd(String key, int dbIndex, String... values) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sadd(key, values);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove multiple values to a set by specified key<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param values  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of values removed successfully <br>
     * @author packy <br>
     */
    public Long srem(String key, int dbIndex, String... values) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.srem(key, values);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Randomly remove value from a set by specified key<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return OK if success <br>
     * @author packy <br>
     */
    public String spop(String key, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.spop(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get difference set of many sets<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param keys    <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return difference set <br>
     * @author packy <br>
     */
    public Set<String> sdiff(int dbIndex, String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sdiff(keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get difference set of many sets, and store new set to another set<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param keys    <br>
     * @param dstKey  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of stored values <br>
     * @author packy <br>
     */
    public Long sdiffstore(String dstKey, int dbIndex, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sdiffstore(dstKey, keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get intersection of many sets <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param keys    <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return intersection <br>
     * @author packy <br>
     */
    public Set<String> sinter(int dbIndex, String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sinter(keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get intersection of many sets, and store new set to another set<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param keys    <br>
     * @param dstKey  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of stored values <br>
     * @author packy <br>
     */
    public Long sinterstore(String dstKey, int dbIndex, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sinterstore(dstKey, keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }


    /**
     * Description: Get union set of many sets <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param keys    <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return union set <br>
     * @author packy <br>
     */
    public Set<String> sunion(int dbIndex, String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sunion(keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get union set of many sets, and store new set to another set<br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param keys    <br>
     * @param dstKey  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of stored values <br>
     * @author packy <br>
     */
    public Long sunionstore(String dstKey, int dbIndex, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sunionstore(dstKey, keys);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Move value specified of one set to another set <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param srcKey  <br>
     * @param dstKey  <br>
     * @param value   Value of src set <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return 1 if success, or 0 if value doesn't exist <br>
     * @author packy <br>
     */
    public Long smove(String srcKey, String dstKey, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.smove(srcKey, dstKey, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get the number of values in one set by specified key <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of values <br>
     * @author packy <br>
     */
    public Long scard(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.scard(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Check whether is the value in the set <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return true if value is in the set <br>
     * @author packy <br>
     */
    public Boolean sismember(String key, String value, int dbIndex) {
        Jedis jedis = null;
        Boolean res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.sismember(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Randomly get value from a set by specified key, but not remove <br>
     * CreateTime 2019-05-25 15:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return value <br>
     * @author packy <br>
     */
    public String srandmember(String key, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.srandmember(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get all values from a set by specified key <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return value <br>
     * @author packy <br>
     */
    public Set<String> smembers(String key, int dbIndex) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.smembers(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Add value to zset with score which is for sorting by specified key <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key     <br>
     * @param score   Used for sorting <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return value <br>
     * @author packy <br>
     */
    public Long zadd(String key, double score, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zadd(key, score, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get all values from a zset between a specified range. start=0, end=-1 means retrieving all values <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key     <br>
     * @param start   <br>
     * @param end     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return matched values <br>
     * @author packy <br>
     */
    public Set<String> zrange(String key, long start, long end, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.zrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return null;
    }

    /**
     * Description: Calculate the number of value from a zset between a specified range <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key     <br>
     * @param start   <br>
     * @param end     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of value, or 0 if error <br>
     * @author packy <br>
     */
    public Long zcount(String key, double start, double end, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.zcount(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }

    }

    /**
     * Description: Increase value with a increment number in a hash for specified field, but only for number <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key       <br>
     * @param field     <br>
     * @param increment <br>
     * @param dbIndex   DB index from 0 to 15 <br>
     * @return Return calculated number, or 0 if error <br>
     * @author packy <br>
     */
    public Long hincrBy(String key, String field, long increment, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.hincrBy(key, field, increment);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool);
        }

    }

    /**
     * Description: Remove specified values from zset <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key     <br>
     * @param values  <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of removed value <br>
     * @author packy <br>
     */
    public Long zrem(String key, int dbIndex, String... values) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zrem(key, values);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Increase score of value in zset <br>
     * CreateTime 2019-05-26 00:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param score   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of removed value <br>
     * @author packy <br>
     */
    public Double zincrby(String key, double score, String value, int dbIndex) {
        Jedis jedis = null;
        Double res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zincrby(key, score, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return asc order of value in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return order of value <br>
     * @author packy <br>
     */
    public Long zrank(String key, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zrank(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return desc order of value in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return order of value <br>
     * @author packy <br>
     */
    public Long zrevrank(String key, String value, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zrevrank(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return desc order of value between specified range in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key     <br>
     * @param start   <br>
     * @param end     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return values <br>
     * @author packy <br>
     */
    public Set<String> zrevrange(String key, long start, long end, int dbIndex) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return desc order of value between specified score range in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key      <br>
     * @param scoreMax <br>
     * @param scoreMin <br>
     * @param dbIndex  DB index from 0 to 15 <br>
     * @return Return values <br>
     * @author packy <br>
     */
    public Set<String> zrangebyscore(String key, String scoreMax, String scoreMin, int dbIndex) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zrevrangeByScore(key, scoreMax, scoreMin);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return the number of values between specified score range in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key      <br>
     * @param scoreMax <br>
     * @param scoreMin <br>
     * @param dbIndex  DB index from 0 to 15 <br>
     * @return Return the number of values <br>
     * @author packy <br>
     */
    public Long zcount(String key, String scoreMin, String scoreMax, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zcount(key, scoreMin, scoreMax);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return the number of values in a zset by specified key <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return the number of values <br>
     * @author packy <br>
     */
    public Long zcard(String key, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zcard(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get score of value in a zset by specified key <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key     <br>
     * @param value   <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return score of value <br>
     * @author packy <br>
     */
    public Double zscore(String key, String value, int dbIndex) {
        Jedis jedis = null;
        Double res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zscore(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove values between specified range in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key     <br>
     * @param start   <br>
     * @param end     <br>
     * @param dbIndex DB index from 0 to 15 <br>
     * @return Return removed value count <br>
     * @author packy <br>
     */
    public Long zremrangeByRank(String key, long start, long end, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Remove values between specified score range in a zset <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key      <br>
     * @param start <br>
     * @param end <br>
     * @param dbIndex  DB index from 0 to 15 <br>
     * @return Return the number of removed values <br>
     * @author packy <br>
     */
    public Long zremrangeByScore(String key, double start, double end, int dbIndex) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Return all pattern matched keys <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param pattern <br>
     * @param dbIndex  DB index from 0 to 15 <br>
     * @return Return all matched keys <br>
     * @author packy <br>
     */
    public Set<String> keys(String pattern, int dbIndex) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.keys(pattern);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Get type of specified key <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param key <br>
     * @param dbIndex  DB index from 0 to 15 <br>
     * @return Return type of key <br>
     * @author packy <br>
     */
    public String type(String key, int dbIndex) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            res = jedis.type(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool);
        }
        return res;
    }

    /**
     * Description: Release collection <br>
     * CreateTime 2019-05-26 09:45 <br>
     *
     * @param jedisPool  DB index from 0 to 15 <br>
     * @return Return type of key <br>
     * @author packy <br>
     */
    private static void returnResource(JedisPool jedisPool) {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

}
