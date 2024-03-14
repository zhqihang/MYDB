package com.qihang.qhdb.backend.vm;

import com.qihang.qhdb.backend.common.AbstractCache;
import com.qihang.qhdb.backend.dm.DataManager;
import com.qihang.qhdb.backend.tm.TransactionManager;
import com.qihang.qhdb.backend.tm.TransactionManagerImpl;
import com.qihang.qhdb.backend.utils.Panic;
import com.qihang.qhdb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: zhqihang
 * @Date: 2024/03/14
 * @Project: qhdb
 * @Description: VM 实现类
 *
 * 设计为 Entry 的缓存，需要继承 AbstractCache<Entry>
 *
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm; // 事务管理器
    DataManager dm; // 数据管理器
    Map<Long, Transaction> activeTransaction; // 活跃事务
    Lock lock; // 锁
    LockTable lt;  // 死锁检测表

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /**
     * 读取一个 entry，注意判断下可见性即可
     *
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * 插入数据，将数据包裹成entry，交给DM进行插入即可
     *
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        // 包裹成entry交给dm处理
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    /**
     * 删除操作只有一个设置 XMAX
     *
     * 需要的前置的三件事：
     *  1. 可见性判断
     *  2. 获取资源的锁
     *  3. 版本跳跃判断
     *
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        // 可见性判断
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);                       // 添加到死锁检测
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);         // 自动回滚
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }
            // 版本跳跃判断
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            // 删除操作
            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
     *
     * @param level 隔离等级
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            // 开启一个新事务
            long xid = tm.begin();
            // 初始化事务的结构
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            // 将其存放在 activeTransaction 中，用于检查和快照使用
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，修改 TM 状态
     *
     * @param xid
     * @throws Exception
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    // 手动回滚
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 自动回滚
     * 1. 在事务被检测出出现死锁时，会自动撤销回滚事务；
     * 2. 出现版本跳跃时，也会自动回滚
     *
     * @param xid 事务id
     * @param autoAborted 是否自动回滚
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    // 释放Entry缓存
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    /**
     * 获取到缓存
     *
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    /**
     * 从缓存释放
     * @param entry
     */
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

}