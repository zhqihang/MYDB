package com.qihang.qhdb.backend.dm.PageCache;

import com.qihang.qhdb.backend.dm.page.Page;

/**
 * @Author: zhqihang
 * @Date: 2024/03/12
 * @Project: qhdb
 * @Description: 页面缓存接口
 */
public interface PageCache {

    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);
}
