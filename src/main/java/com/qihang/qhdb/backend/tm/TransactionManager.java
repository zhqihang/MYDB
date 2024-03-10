package com.qihang.qhdb.backend.tm;

import com.qihang.qhdb.backend.common.Error;
import com.qihang.qhdb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author: zhqihang
 * @Date: 2024/03/10
 * @Project: qhdb
 * @Description: TM 通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
 */
public interface TransactionManager {


    // 提供接口 供其他模块调用, 用来创建事务和查询事务状态

    long begin(); // 开启一个事务

    void commit(long xid); // 提交一个事务

    void abort(long xid);  // 取消一个事务

    boolean isActive(long xid); // 查询一个事务的状态是否是正在进行

    boolean isCommitted(long xid); // 查询一个事务是否已提交

    boolean isAbort(long xid); // 查询一个事务是否已取消

    void close(); // 关闭TM

    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                // 文件已存在
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            // 文件不可读或不可写
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
