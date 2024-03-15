package com.qihang.qhdb.transport;

/**
 * NYDB 被设计为 C/S 结构，类似于 MySQL。
 * 支持启动一个服务器，并有多个客户端去连接，通过 socket 通信，执行 SQL 返回结果。
 *
 * MYDB 使用了一种特殊的二进制格式，用于客户端和服务端通信
 *
 */

// 将sql语句和错误一起打包
public class Package {
    byte[] data;    // sql语句
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
