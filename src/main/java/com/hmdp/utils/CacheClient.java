package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * @author smin
 * @date 2023/5/15
 * @Description 基于StringRedisTemplate封装一个工具类，满足下列需求
 *              方法1：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
 *              方法2：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
 *              方法3：根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
 *              方法4：根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
 */
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    private static ExecutorService threadPool = new ThreadPoolExecutor(
            2,
            5,
            3,
            TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(3),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public <T> void set(String key, T value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * @Param key:
     * @Param value:
     * @Param time:
     * @Param unit:
     * @return: void
     * description: 设置逻辑过期
     */
    public <T> void setWithLogicalExpire(String key, T value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * @Param keyPrefix: 
     * @Param id: 
     * @Param type: 
     * @Param dbFallback: 
     * @Param time: 
     * @Param unit: 
     * @return: R
     * description: 设置空值解决缓存穿透问题
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key=keyPrefix+id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        if (json!=null){
            return null;
        }
        R r = dbFallback.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        this.set(key, r, time, unit);
        return r;

    }

    /**
     * @Param keyPrefix:
     * @Param id:
     * @Param type:
     * @Param dbFallback:
     * @Param time:
     * @Param unit:
     * @return: R
     * description: 利用逻辑过期解决缓存击穿问题
     */
    public <R,ID> R queryWithLocalExpire(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)){
            return null;
        }
        //命中，将json反序列化成对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期
        if (expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //已经过期
        String lockKey=LOCK_SHOP_KEY+id;
        boolean isLock=tryLock(lockKey);
        if (isLock){
            threadPool.submit(()->{
                try {
                    //查询数据库
                    R newR= dbFallback.apply(id);
                    //重建缓存
                    this.setWithLogicalExpire(key,newR,time,unit);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unLock(key);
                }
            });
        }
        return r;
    }


    /**
     * @Param keyPrefix:
     * @Param id:
     * @Param type:
     * @Param dbFallback:
     * @Param time:
     * @Param unit:
     * @return: R
     * description: 利用互斥锁解决缓存击穿问题
     */
    public <R,ID> R queryWithMutex(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson,type);
        }
        if (shopJson!=null){
            return null;
        }
        //实现缓存重建
        String lockKey=LOCK_SHOP_KEY+id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock){
                Thread.sleep(50);
                return queryWithMutex(keyPrefix,id,type,dbFallback,time,unit);
            }
            r=dbFallback.apply(id);
            //数据库中不存在
            if (r==null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            this.set(key,r,time,unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unLock(lockKey);
        }
        return r;
    }

    /**
     * @Param key:
     * @return: boolean
     * description:尝试获取锁
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return flag;
    }

    /**
     * @Param key:
     * @return: void
     * description: 解锁
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

}
