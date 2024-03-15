package com.qihang.qhdb.backend;

import com.qihang.qhdb.common.Error;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.qihang.qhdb.backend.dm.DataManager;
import com.qihang.qhdb.backend.server.Server;
import com.qihang.qhdb.backend.tbm.TableManager;
import com.qihang.qhdb.backend.tm.TransactionManager;
import com.qihang.qhdb.backend.utils.Panic;
import com.qihang.qhdb.backend.vm.VersionManager;
import com.qihang.qhdb.backend.vm.VersionManagerImpl;

/**
 * @Author: zhqihang
 * @Date: 2024/03/15
 * @Project: qhdb
 * @Description:
 *
 * 服务器的启动入口。这个类解析了命令行参数。很重要的参数就是 -open 或者 -create。
 * Launcher 根据两个参数，来决定是创建数据库文件，还是启动一个已有的数据库。
 *
 */
public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    // 创建数据库文件
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);    // 新建tm
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm); // 新建dm
        VersionManager vm = new VersionManagerImpl(tm, dm);         // 新建vm
        TableManager.create(path, vm, dm);                          // 新建tbm
        tm.close();
        dm.close();
    }

    // 开启数据库文件
    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);      // 打开tm
        DataManager dm = DataManager.open(path, mem, tm);           // 打开dm
        VersionManager vm = new VersionManagerImpl(tm, dm);         // 打开vm
        TableManager tbm = TableManager.open(path, vm, dm);         // 打开tbm
        new Server(port, tbm).start();                              // 打开sql服务器
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}