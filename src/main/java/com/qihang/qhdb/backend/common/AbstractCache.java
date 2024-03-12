package com.qihang.qhdb.backend.common;

import com.qihang.qhdb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: zhqihang
 * @Date: 2024/03/12
 * @Project: qhdb
 * @Description: AbstractCache 实现了一个引用计数策略的缓存
 *
 * 其他的缓存只需要继承这个类，并实现那两个抽象方法即可
 *
 */
public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache; // 实际缓存数据
    private HashMap<Long, Integer> references; // 资源引用的个数
    private HashMap<Long, Boolean> getting; // 正在被获取的资源

    private int maxResource; // 缓存中的最大资源数
    private int count = 0; // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 获取缓存
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        // 尝试从缓存中获取
        while (true) {
            lock.lock();
            // 检查此时是否有其他线程正在从数据源获取这个资源
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            // 资源在缓存中，直接返回
            if (cache.containsKey(key)) {

                T obj = cache.get(key);
                // 资源引用数+1
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            // 资源不在缓存中，尝试获取该资源
            if (maxResource > 0 && count == maxResource) {
                // 如果缓存满了 抛出异常
                lock.unlock();
                throw Error.CacheFullException;
            }
            //如果没满，getting上注册，线程准备获取资源
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        T obj = null;
        try {
            obj = getForCache(key); // 调用抽象方法，获取资源
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        // 获取完成后，从getting中删除key，并写入缓存，引用数设置为 1
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     *
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            // ref 释放后的引用数
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else {
                references.put(key, ref);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐是的写回行为
     *
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
