package com.gearstation.eldercare.cache.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
        }
    }

    /**
     * Description: Clear all keys in current DB <br>
     * CreateTime 2019-05-14 23:45 <br>
     *
     * @return Always return OK <br>
     * @author packy <br>
     **/
    public String flushDB() {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.flushDB();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
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
     **/
    public Long expire(String key, int time, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.expire(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
    public String setex(String key, String value, int seconds, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.setex(key, seconds, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
    public String getSet(String key, String value, int dbIndex) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.getSet(key, value);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
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
     **/
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
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key给指定的value加值,如果key不存在,则这是value为该值
     * </p>
     *
     * @param key
     * @param integer
     * @return
     */
    public Long incrBy(String key, Long integer) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.incrBy(key, integer);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 对key的值做减减操作,如果key不存在,则设置key为-1
     * </p>
     *
     * @param key
     * @return
     */
    public Long decr(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.decr(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 减去指定的值
     * </p>
     *
     * @param key
     * @param integer
     * @return
     */
    public Long decrBy(String key, Long integer) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.decrBy(key, integer);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取value值的长度
     * </p>
     *
     * @param key
     * @return 失败返回null
     */
    public Long serlen(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.strlen(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key给field设置指定的值,如果key不存在,则先创建
     * </p>
     *
     * @param key
     * @param field 字段
     * @param value
     * @return 如果存在返回0 异常返回null
     */
    public Long hset(String key, String field, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hset(key, field, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key给field设置指定的值,如果key不存在则先创建,如果field已经存在,返回0
     * </p>
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public Long hsetnx(String key, String field, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hsetnx(key, field, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key同时设置 hash的多个field
     * </p>
     *
     * @param key
     * @param hash
     * @return 返回OK 异常返回null
     */
    public String hmset(String key, Map<String, String> hash, int indexdb) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.hmset(key, hash);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key 和 field 获取指定的 value
     * </p>
     *
     * @param key
     * @param field
     * @return 没有返回null
     */
    public String hget(String key, String field) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hget(key, field);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key 和 fields 获取指定的value 如果没有对应的value则返回null
     * </p>
     *
     * @param key
     * @param fields 可以使 一个String 也可以是 String数组
     * @return
     */
    public List<String> hmget(String key, int indexdb, String... fields) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.hmget(key, fields);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key给指定的field的value加上给定的值
     * </p>
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public Long hincrby(String key, String field, Long value) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hincrBy(key, field, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key和field判断是否有指定的value存在
     * </p>
     *
     * @param key
     * @param field
     * @return
     */
    public Boolean hexists(String key, String field) {
        Jedis jedis = null;
        Boolean res = false;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hexists(key, field);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回field的数量
     * </p>
     *
     * @param key
     * @return
     */
    public Long hlen(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hlen(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;

    }

    /**
     * <p>
     * 通过key 删除指定的 field
     * </p>
     *
     * @param key
     * @param fields 可以是 一个 field 也可以是 一个数组
     * @return
     */
    public Long hdel(String key, String... fields) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hdel(key, fields);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回所有的field
     * </p>
     *
     * @param key
     * @return
     */
    public Set<String> hkeys(String key) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hkeys(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回所有和key有关的value
     * </p>
     *
     * @param key
     * @return
     */
    public List<String> hvals(String key) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.hvals(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取所有的field和value
     * </p>
     *
     * @param key
     * @return
     */
    public Map<String, String> hgetall(String key, int indexdb) {
        Jedis jedis = null;
        Map<String, String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.hgetAll(key);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key向list头部添加字符串
     * </p>
     *
     * @param key
     * @param strs 可以使一个string 也可以使string数组
     * @return 返回list的value个数
     */
    public Long lpush(int indexdb, String key, String... strs) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.lpush(key, strs);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key向list尾部添加字符串
     * </p>
     *
     * @param key
     * @param strs 可以使一个string 也可以使string数组
     * @return 返回list的value个数
     */
    public Long rpush(String key, String... strs) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.rpush(key, strs);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key在list指定的位置之前或者之后 添加字符串元素
     * </p>
     *
     * @param key
     * @param where LIST_POSITION枚举类型
     * @param pivot list里面的value
     * @param value 添加的value
     * @return
     */
    public Long linsert(String key, LIST_POSITION where, String pivot,
                        String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.linsert(key, where, pivot, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key设置list指定下标位置的value
     * </p>
     * <p>
     * 如果下标超过list里面value的个数则报错
     * </p>
     *
     * @param key
     * @param index 从0开始
     * @param value
     * @return 成功返回OK
     */
    public String lset(String key, Long index, String value) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.lset(key, index, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key从对应的list中删除指定的count个 和 value相同的元素
     * </p>
     *
     * @param key
     * @param count 当count为0时删除全部
     * @param value
     * @return 返回被删除的个数
     */
    public Long lrem(String key, long count, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.lrem(key, count, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key保留list中从strat下标开始到end下标结束的value值
     * </p>
     *
     * @param key
     * @param start
     * @param end
     * @return 成功返回OK
     */
    public String ltrim(String key, long start, long end) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.ltrim(key, start, end);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key从list的头部删除一个value,并返回该value
     * </p>
     *
     * @param key
     * @return
     */
    synchronized public String lpop(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.lpop(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key从list尾部删除一个value,并返回该元素
     * </p>
     *
     * @param key
     * @return
     */
    synchronized public String rpop(String key, int indexdb) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.rpop(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key从一个list的尾部删除一个value并添加到另一个list的头部,并返回该value
     * </p>
     * <p>
     * 如果第一个list为空或者不存在则返回null
     * </p>
     *
     * @param srckey
     * @param dstkey
     * @return
     */
    public String rpoplpush(String srckey, String dstkey, int indexdb) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.rpoplpush(srckey, dstkey);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取list中指定下标位置的value
     * </p>
     *
     * @param key
     * @param index
     * @return 如果没有返回null
     */
    public String lindex(String key, long index) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.lindex(key, index);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回list的长度
     * </p>
     *
     * @param key
     * @return
     */
    public Long llen(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.llen(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取list指定下标位置的value
     * </p>
     * <p>
     * 如果start 为 0 end 为 -1 则返回全部的list中的value
     * </p>
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<String> lrange(String key, long start, long end, int indexdb) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(indexdb);
            res = jedis.lrange(key, start, end);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 将列表 key 下标为 index 的元素的值设置为 value
     * </p>
     *
     * @param key
     * @param index
     * @param value
     * @return 操作成功返回 ok ，否则返回错误信息
     */
    public String lset(String key, long index, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lset(key, index, value);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return null;
    }

    /**
     * <p>
     * 返回给定排序后的结果
     * </p>
     *
     * @param key
     * @param sortingParameters
     * @return 返回列表形式的排序结果
     */
    public List<String> sort(String key, SortingParams sortingParameters) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sort(key, sortingParameters);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return null;
    }

    /**
     * <p>
     * 返回排序后的结果，排序默认以数字作为对象，值被解释为双精度浮点数，然后进行比较。
     * </p>
     *
     * @param key
     * @return 返回列表形式的排序结果
     */
    public List<String> sort(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sort(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return null;
    }

    /**
     * <p>
     * 通过key向指定的set中添加value
     * </p>
     *
     * @param key
     * @param members 可以是一个String 也可以是一个String数组
     * @return 添加成功的个数
     */
    public Long sadd(String key, String... members) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sadd(key, members);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key删除set中对应的value值
     * </p>
     *
     * @param key
     * @param members 可以是一个String 也可以是一个String数组
     * @return 删除的个数
     */
    public Long srem(String key, String... members) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.srem(key, members);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key随机删除一个set中的value并返回该值
     * </p>
     *
     * @param key
     * @return
     */
    public String spop(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.spop(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取set中的差集
     * </p>
     * <p>
     * 以第一个set为标准
     * </p>
     *
     * @param keys 可以使一个string 则返回set中所有的value 也可以是string数组
     * @return
     */
    public Set<String> sdiff(String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sdiff(keys);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取set中的差集并存入到另一个key中
     * </p>
     * <p>
     * 以第一个set为标准
     * </p>
     *
     * @param dstkey 差集存入的key
     * @param keys   可以使一个string 则返回set中所有的value 也可以是string数组
     * @return
     */
    public Long sdiffstore(String dstkey, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sdiffstore(dstkey, keys);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取指定set中的交集
     * </p>
     *
     * @param keys 可以使一个string 也可以是一个string数组
     * @return
     */
    public Set<String> sinter(String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sinter(keys);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取指定set中的交集 并将结果存入新的set中
     * </p>
     *
     * @param dstkey
     * @param keys   可以使一个string 也可以是一个string数组
     * @return
     */
    public Long sinterstore(String dstkey, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sinterstore(dstkey, keys);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回所有set的并集
     * </p>
     *
     * @param keys 可以使一个string 也可以是一个string数组
     * @return
     */
    public Set<String> sunion(String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sunion(keys);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回所有set的并集,并存入到新的set中
     * </p>
     *
     * @param dstkey
     * @param keys   可以使一个string 也可以是一个string数组
     * @return
     */
    public Long sunionstore(String dstkey, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sunionstore(dstkey, keys);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key将set中的value移除并添加到第二个set中
     * </p>
     *
     * @param srckey 需要移除的
     * @param dstkey 添加的
     * @param member set中的value
     * @return
     */
    public Long smove(String srckey, String dstkey, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.smove(srckey, dstkey, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取set中value的个数
     * </p>
     *
     * @param key
     * @return
     */
    public Long scard(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.scard(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key判断value是否是set中的元素
     * </p>
     *
     * @param key
     * @param member
     * @return
     */
    public Boolean sismember(String key, String member) {
        Jedis jedis = null;
        Boolean res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.sismember(key, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取set中随机的value,不删除元素
     * </p>
     *
     * @param key
     * @return
     */
    public String srandmember(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.srandmember(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取set中所有的value
     * </p>
     *
     * @param key
     * @return
     */
    public Set<String> smembers(String key) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.smembers(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key向zset中添加value,score,其中score就是用来排序的
     * </p>
     * <p>
     * 如果该value已经存在则根据score更新元素
     * </p>
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zadd(key, score, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 返回有序集 key 中，指定区间内的成员。min=0,max=-1代表所有元素
     * </p>
     *
     * @param key
     * @param min
     * @param max
     * @return 指定区间内的有序集成员的列表。
     */
    public Set<String> zrange(String key, long min, long max) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrange(key, min, max);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return null;
    }

    /**
     * <p>
     * 统计有序集 key 中,值在 min 和 max 之间的成员的数量
     * </p>
     *
     * @param key
     * @param min
     * @param max
     * @return 值在 min 和 max 之间的成员的数量。异常返回0
     */
    public Long zcount(String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcount(key, min, max);
        } catch (Exception e) {

            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool, jedis);
        }

    }

    /**
     * <p>
     * 为哈希表 key 中的域 field 的值加上增量 increment 。增量也可以为负数，相当于对给定域进行减法操作。 如果 key
     * 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。本操作的值被限制在 64 位(bit)有符号数字表示之内。
     * </p>
     * <p>
     * 将名称为key的hash中field的value增加integer
     * </p>
     *
     * @param key
     * @param value
     * @param increment
     * @return 执行 HINCRBY 命令之后，哈希表 key 中域 field的值。异常返回0
     */
    public Long hincrBy(String key, String value, long increment) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hincrBy(key, value, increment);
        } catch (Exception e) {
            log.error(e.getMessage());
            return 0L;
        } finally {
            returnResource(jedisPool, jedis);
        }

    }

    /**
     * <p>
     * 通过key删除在zset中指定的value
     * </p>
     *
     * @param key
     * @param members 可以使一个string 也可以是一个string数组
     * @return
     */
    public Long zrem(String key, String... members) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zrem(key, members);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key增加该zset中value的score的值
     * </p>
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public Double zincrby(String key, double score, String member) {
        Jedis jedis = null;
        Double res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zincrby(key, score, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回zset中value的排名
     * </p>
     * <p>
     * 下标从小到大排序
     * </p>
     *
     * @param key
     * @param member
     * @return
     */
    public Long zrank(String key, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zrank(key, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回zset中value的排名
     * </p>
     * <p>
     * 下标从大到小排序
     * </p>
     *
     * @param key
     * @param member
     * @return
     */
    public Long zrevrank(String key, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zrevrank(key, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key将获取score从start到end中zset的value
     * </p>
     * <p>
     * socre从大到小排序
     * </p>
     * <p>
     * 当start为0 end为-1时返回全部
     * </p>
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<String> zrevrange(String key, long start, long end) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zrevrange(key, start, end);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回指定score内zset中的value
     * </p>
     *
     * @param key
     * @param max
     * @param min
     * @return
     */
    public Set<String> zrangebyscore(String key, String max, String min) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回指定score内zset中的value
     * </p>
     *
     * @param key
     * @param max
     * @param min
     * @return
     */
    public Set<String> zrangeByScore(String key, double max, double min) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 返回指定区间内zset中value的数量
     * </p>
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zcount(String key, String min, String max) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zcount(key, min, max);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key返回zset中的value个数
     * </p>
     *
     * @param key
     * @return
     */
    public Long zcard(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zcard(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key获取zset中value的score值
     * </p>
     *
     * @param key
     * @param member
     * @return
     */
    public Double zscore(String key, String member) {
        Jedis jedis = null;
        Double res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zscore(key, member);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key删除给定区间内的元素
     * </p>
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Long zremrangeByRank(String key, long start, long end) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 通过key删除指定score内的元素
     * </p>
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Long zremrangeByScore(String key, double start, double end) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * <p>
     * 返回满足pattern表达式的所有key
     * </p>
     * <p>
     * keys(*)
     * </p>
     * <p>
     * 返回所有的key
     * </p>
     *
     * @param pattern
     * @return
     */
    public Set<String> keys(String pattern) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.keys(pattern);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    public Set<String> keysBySelect(String pattern, int database) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(database);
            res = jedis.keys(pattern);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }


    /**
     * <p>
     * 通过key判断值得类型
     * </p>
     *
     * @param key
     * @return
     */
    public String type(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            jedis = jedisPool.getResource();
            res = jedis.type(key);
        } catch (Exception e) {

            log.error(e.getMessage());
        } finally {
            returnResource(jedisPool, jedis);
        }
        return res;
    }

    /**
     * 序列化对象
     *
     * @param obj
     * @return 对象需实现Serializable接口
     */
    public static byte[] ObjTOSerialize(Object obj) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream byteOut = null;
        try {
            byteOut = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(byteOut);
            oos.writeObject(obj);
            byte[] bytes = byteOut.toByteArray();
            return bytes;
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 反序列化对象
     *
     * @param bytes
     * @return 对象需实现Serializable接口
     */
    public static Object unserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        try {
            //反序列化
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 返还到连接池
     *
     * @param jedisPool
     * @param jedis
     */
    public static void returnResource(JedisPool jedisPool, Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }

}
