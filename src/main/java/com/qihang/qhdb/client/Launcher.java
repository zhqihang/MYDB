package com.qihang.qhdb.client;

import com.qihang.qhdb.transport.Encoder;
import com.qihang.qhdb.transport.Packager;
import com.qihang.qhdb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 客户端的启动入口，就是连接上服务器跑一个shell类
 */
public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}