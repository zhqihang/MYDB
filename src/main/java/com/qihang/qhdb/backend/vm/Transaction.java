package com.qihang.qhdb.backend.vm;

import com.qihang.qhdb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

// vm对其他模块提供的一个抽象的事务数据结构 以保存快照数据


public class Transaction {
    public long xid;                    // 事务id
    public int level;                   // 事务隔离等级，0：读已提交；1：可重复读
    public Map<Long, Boolean> snapshot; // 活跃事务的快照，用于实现可重复读
    public Exception err;               // 事务的错误
    public boolean autoAborted;         // 自动回滚标记


    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 只有可重复读才需要 活跃事务列表
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    // 判断xid是否是活跃事务
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
