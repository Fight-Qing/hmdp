package com.hmdp.utils;

/**
 * @author smin
 * @date 2023/5/26
 * @Description
 */
public interface ILock {

    /**
     * @Param timeoutSec:
     * @return: boolean
     * description: 尝试获取锁
     */
    boolean tryLock(long timeoutSec);

    /**
     * @return: void
     * description: 释放锁
     */
    void unlock();
}
