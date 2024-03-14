package com.qihang.qhdb.backend.dm.pageIndex;

import com.qihang.qhdb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {

    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的内存大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;

    // 维护一个页面信息的 List数组，实现页面索引
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 插入页面操作
     * 前面被选择的页，会直接从 PageIndex 中移除，这意味着，同一个页面是不允许并发写的。
     * 在上层模块使用完这个页面后，需要将其重新插入 PageIndex
     *
     * @param pgno      页号
     * @param freeSpace 空闲空间大小
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从 PageIndex 中获取页面
     * 算出区间号，直接取
     *
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 计算出满足请求空间的区间号
            int number = spaceSize / THRESHOLD;
            // 因为区间从1开始，所以要加1操作
            if (number < INTERVALS_NO) number++;
            // 循环查找大于等于请求空间的页面
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    // 当前区间没有空余页面，后移一个区间
                    number++;
                    continue;
                }
                // 从页面索引List中移除第一个满足的页面信息PageInfo
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
