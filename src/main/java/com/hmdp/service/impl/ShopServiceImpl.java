package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private CacheClient cacheClient;

    private static ExecutorService threadPool = new ThreadPoolExecutor(
            2,
            5,
            3,
            TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(3),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.DiscardOldestPolicy());


    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
       Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //逻辑过期解决缓存击穿
//        Shop shop = cacheClient.queryWithLocalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //互斥搜解决缓存击穿
//        Shop shop = cacheClient.queryWithMutex(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if (shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    /**
     * @Param id: 
     * @return: com.hmdp.dto.Result
     * description: 缓存null值解决商品查询的缓存穿透问题
     */
    public Shop queryWithPassThrough(Long id) {
        String key=CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //从redis查询缓存命中
        if (shopJson!=null){
            if (StrUtil.isNotBlank(shopJson)) {
                Shop shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            return null;
        }
        Shop shop = getById(id);
        if (shop==null){
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /**
     * @Param id:
     * @return: com.hmdp.entity.Shop
     * description: 利用互斥锁解决缓存击穿问题
     */
    public Shop queryWithMutex(Long id)  {
        String key = CACHE_SHOP_KEY + id;
        // 1、从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get("key");
        // 2、判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 存在,直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的值是否是空值
        if (shopJson != null) {
            //返回一个错误信息
            return null;
        }
        // 4.实现缓存重构
        //4.1 获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2 判断否获取成功
            if(!isLock){
                //4.3 失败，则休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4 成功，根据id查询数据库
            shop = getById(id);
            // 5.不存在，返回错误
            if(shop == null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //6.写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_NULL_TTL,TimeUnit.MINUTES);

        }catch (Exception e){
            throw new RuntimeException(e);
        }
        finally {
            //7.释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }

    /**
     * @Param id:
     * @return: com.hmdp.entity.Shop
     * description: 利用逻辑过期解决缓存击穿问题
     *              当用户开始查询redis时，判断是否命中，如果没有命中则直接返回空数据，理论来讲缓存不会出现未命中的情况，因为加的是逻辑过期时间，其实不会过期
     *              一般这种活动都会预热，提前加入缓存
     */
    public Shop queryWithLogicalExpire( Long id ){

        String key=CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(shopJson)){
            return null;
        }
        RedisData redisData = JSONUtil.toBean(shopJson,RedisData.class);
        Shop shop=JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())){
            return shop;
        }
        String lockKey=LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        if (isLock){
            threadPool.submit(()->{
                try {
                    saveShopToRedis(id,20L);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unLock(lockKey);
                }
            });
        }
        return shop;
    }




    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id==null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
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

    private void saveShopToRedis(Long id,Long expireSeconds){
        //1.查询数据库
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData=new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
}
