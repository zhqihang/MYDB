package com.qihang.qhdb.backend.vm;

import com.google.common.primitives.Bytes;
import com.qihang.qhdb.backend.common.SubArray;
import com.qihang.qhdb.backend.dm.dataItem.DataItem;
import com.qihang.qhdb.backend.utils.Parser;

import java.util.Arrays;
/**
 * @Author: zhqihang
 * @Date: 2024/03/14
 * @Project: qhdb
 * @Description:
 *
 * VM向上层抽象出entry，用于记录数据版本链
 * entry结构：
 *      [XMIN] [XMAX] [data]
 *      8byte  8byte
 *      XMIN：创建该条记录（版本）的事务编号
 *      XMAX：删除该条记录（版本）的事务编号 (DM 没有删除操作的原因, 只需要设置其 XMAX, 等价于删除)
 *      DATA：这条记录持有的数据
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;           // 版本id
    private DataItem dataItem;  // 数据项
    private VersionManager vm;  // 事物的版本管理器

    // 读取一个 DataItem 打包成 entry
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 将事务id和数据记录打包成一个 entry格式
     *
     * @param xid 事务id
     * @param data 记录数据
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    /**
     * 获取记录中持有的数据
     * 以拷贝的形式返回内容
     *
     * @return
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 修改数据
     * @param xid
     */
    public void setXmax(long xid) {
        dataItem.before(); // 修改数据前必须执行 包含了加写锁，设置脏页面，暂存需要修改的数据内容到oldRaw
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid); // 修改数据后必须执行 记录此事务的修改操作到日志，关闭写锁
        }
    }

    public long getUid() {
        return uid;
    }
}
