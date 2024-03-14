package com.qihang.qhdb.backend.vm;

import com.qihang.qhdb.backend.dm.DataManager;
import com.qihang.qhdb.backend.tm.TransactionManager;

/**
 * @Author: zhqihang
 * @Date: 2024/03/14
 * @Project: qhdb
 * @Description: ...
 */
public interface VersionManager {

    // 数据版本链管理
    byte[] read(long xid, long uid) throws Exception;       // 保证可见性的条件下，读取数据DataItem
    long insert(long xid, byte[] data) throws Exception;    // 通过事务xid插入数据
    boolean delete(long xid, long uid) throws Exception;    // 通过事务xid删除数据

    // 事务管理
    long begin(int level);                                  // 事务开启隔离级别
    void commit(long xid) throws Exception;                 // 提交事务
    void abort(long xid);                                   // 撤销事务

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
