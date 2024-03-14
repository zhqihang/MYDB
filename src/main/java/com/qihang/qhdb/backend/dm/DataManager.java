package com.qihang.qhdb.backend.dm;

import com.qihang.qhdb.backend.dm.dataItem.DataItem;
import com.qihang.qhdb.backend.dm.logger.Logger;
import com.qihang.qhdb.backend.dm.page.PageOne;
import com.qihang.qhdb.backend.dm.pageCache.PageCache;
import com.qihang.qhdb.backend.tm.TransactionManager;

/**
 * 数据管理模块接口：
 * 创建 DataManager
 * 1. 空文件创建(create): 对第一页进行初始化
 * 2. 已有文件创建(open): 对第一页进行校验, 判断是否需要执行恢复流程, 并重新对第一页生成随机字节
 */

public interface DataManager {

    DataItem read(long uid) throws Exception; // 读取数据

    long insert(long xid, byte[] data) throws Exception; // 插入数据

    void close(); // 关闭数据管理器

    /**
     * 空文件创建DataManager
     *
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem); // 新建页面缓存
        Logger lg = Logger.create(path); // 新建日志
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm); // 新建 DataManager
        dm.initPageOne(); // 对第一页校验页面 进行初始化
        return dm;
    }

    /**
     * 已有文件新建 DataManager
     *
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem); // 打开页面缓存
        Logger lg = Logger.open(path); // 打开日志
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm); // 新建 DataManager
        // 是否执行恢复流程
        if (!dm.loadCheckPageOne()) {
            // 数据库非正常关闭 执行恢复
            Recover.recover(tm, lg, pc);
        }
        // 重新填写页面索引
        dm.fillPageIndex();
        // 重新设置 第一页 随机字节
        PageOne.setVcOpen(dm.pageOne);
        // 第一页 刷回数据源
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
