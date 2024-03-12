package com.qihang.qhdb.common;

/**
 * @Author: zhqihang
 * @Date: 2024/03/10
 * @Project: qhdb
 * @Description: ...
 */
public class Error {
    // common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");

    // tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

}
