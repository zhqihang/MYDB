package com.qihang.qhdb.backend.dm.page;

/**
 * @Author: zhqihang
 * @Date: 2024/03/12
 * @Project: qhdb
 * @Description: 定义页面结构
 */
public interface Page {

    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
