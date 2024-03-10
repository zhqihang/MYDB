package com.qihang.qhdb.backend.utils;

/**
 * @Author: zhqihang
 * @Date: 2024/03/10
 * @Project: qhdb
 * @Description: ...
 */
public class Panic {

    // 无法恢复的错误 直接停机
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }

}
