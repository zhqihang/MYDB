package com.qihang.qhdb.backend.vm;

import com.qihang.qhdb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 表锁：
 * 维护了一个依赖等待图，以进行死锁检测
 *
 * 上一节提到了 2PL 会阻塞事务，直至持有锁的线程释放锁。
 * 可以将这种等待关系抽象成有向边，例如 Tj 在等待 Ti，就可以表示为 Tj –> Ti。
 * 这样，无数有向边就可以形成一个图（不一定是连通图）。
 * 检测死锁也就简单了，只需要查看这个图中是否有环即可。(深度优先搜索)
 *
 */
public class LockTable {
    
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;
    private Map<Long, Integer> xidStamp;
    private int stamp;                  // 访问戳

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 在每次出现等待的情况时，就尝试向图中增加一条边，并进行死锁检测。
     * 如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务。
     *
     * 向依赖等待图中添加一个等待记录
     * 事务xid 阻塞等待 数据项uid，如果会造成死锁则抛出异常
     *
     * @param xid 事务id
     * @param uid 数据项key
     * @return 不需要等待则返回null，否则返回锁对象
     * @throws Exception
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // dataitem数据已经被事务xid获取到，不需要等待，返回null
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 如果 uid 资源不被持有 xid获得该uid 加入持有列表 不需要等待 返回null
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            waitU.put(xid, uid); // 添加等待状态

            putIntoList(wait, xid, uid); // 加入列表
            // 死锁检测
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 返回上了锁的 Lock 对象
            // 调用方在获取到该对象时，需要尝试获取该对象的锁，由此实现阻塞线程的目的
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除
     *
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid); // 释放的资源可以被获取
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;
        // 从 List 开头开始尝试解锁，还是个公平锁。
        // 解锁时，将该 Lock 对象 unlock 即可，这样业务线程就获取到了锁，就可以继续执行了。
        while(l.size() > 0) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    // 死锁检测
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 深度优先搜索
     *
     * @param xid
     * @return
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
