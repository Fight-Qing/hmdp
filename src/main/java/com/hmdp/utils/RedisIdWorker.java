package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author smin
 * @date 2023/5/20
 * @Description
 */
@Component
public class RedisIdWorker {
    //开始时间戳
    private static final long BEGIN_TIMESTAMP=1684615337L;
    //序列号的位数
    private static int COUNT_BITS=32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate=stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        //生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond=now.toEpochSecond(ZoneOffset.UTC);
        long timesTamp=nowSecond-BEGIN_TIMESTAMP;

        //生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count=stringRedisTemplate.opsForValue().increment("icr:"+keyPrefix+":"+date);

        return timesTamp<<COUNT_BITS|count;
    }
}
