package com.qihang.qhdb.client;

import com.qihang.qhdb.transport.Package;
import com.qihang.qhdb.transport.Packager;

// 实现单次收发动作

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
