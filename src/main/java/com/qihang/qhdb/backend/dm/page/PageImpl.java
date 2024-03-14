package com.qihang.qhdb.backend.dm.page;

import com.qihang.qhdb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: zhqihang
 * @Date: 2024/03/12
 * @Project: qhdb
 * @Description: Page实现类
 */
public class PageImpl implements Page{

    private int pageNumber; // 页面页号
    private byte[] data; // 实际包含的字节数据
    private boolean dirty; // 是否是脏页面
    private Lock lock;

    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
