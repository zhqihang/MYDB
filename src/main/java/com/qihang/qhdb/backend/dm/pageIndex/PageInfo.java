package com.qihang.qhdb.backend.dm.pageIndex;

/**
 * 页面信息类
 */
public class PageInfo {

    public int pgno; // 页号
    public int freeSpace; // 空闲空间

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }

}
