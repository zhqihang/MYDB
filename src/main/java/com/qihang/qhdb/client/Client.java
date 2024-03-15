package com.qihang.qhdb.client;

import com.qihang.qhdb.transport.Packager;
import com.qihang.qhdb.transport.Package;

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    // 接收 shell 发过来的sql语句，并打包成pkg进行单次收发操作，得到执行结果并返回
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}