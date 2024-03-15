package com.qihang.qhdb.backend.parser;

import com.qihang.qhdb.common.Error;

/**
 * @Author: zhqihang
 * @Date: 2024/03/15
 * @Project: qhdb
 * @Description:
 * 语法规则：
 *
 * <begin statement>
 *     begin [isolation level (read committedrepeatable read)]
 *         begin isolation level read committed
 *
 * <commit statement>
 *     commit
 *
 * <abort statement>
 *     abort
 *
 * <create statement>
 *     create table <table name>
 *     <field name> <field type>
 *     <field name> <field type>
 *     ...
 *     <field name> <field type>
 *     [(index <field name list>)]
 *         create table students
 *         id int32,
 *         name string,
 *         age int32,
 *         (index id name)
 *
 * <drop statement>
 *     drop table <table name>
 *         drop table students
 *
 * <select statement>
 *     select (*<field name list>) from <table name> [<where statement>]
 *         select * from student where id = 1
 *         select name from student where id > 1 and id < 4
 *         select name, age, id from student where id = 12
 *
 * <insert statement>
 *     insert into <table name> values <value list>
 *         insert into student values 5 "Zhang Yuanjia" 22
 *
 * <delete statement>
 *     delete from <table name> <where statement>
 *         delete from student where name = "Zhang Yuanjia"
 *
 * <update statement>
 *     update <table name> set <field name>=<value> [<where statement>]
 *         update student set name = "ZYJ" where id = 5
 *
 * <where statement>
 *     where <field name> (><=) <value> [(andor) <field name> (><=) <value>]
 *         where age > 10 or age < 3
 *
 * <field name> <table name>
 *     [a-zA-Z][a-zA-Z0-9_]*
 *
 * <field type>
 *     int32 int64 string
 *
 * <value>
 *     .*
 *
 * 本类对语句进行逐字节解析，根据空白符或者上述词法规则，将语句切割成多个 token。
 * 对外提供了 peek()、pop() 方法方便取出 Token 进行解析。切割的实现不赘述。
 *
 */
public class Tokenizer {
    private byte[] stat;            // 需要解析的字段
    private int pos;                // 指向token的指针
    private String currentToken;    // 当前Token，如果没有pop()，peek()的时候直接返回currentToken即可
    private boolean flushToken;     // 送出token的一个标记，用于调用pop()的标记，防止peek重复读取token
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    // 先peek()，再pop()
    public void pop() {
        flushToken = true;
    }

    // 返回错误的语句，格式就是在正确语句与错误语句之间插入 "<<"
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    // 弹出一个字节，无返回值，需要先使用peekByte()
    private void popByte() {
        pos++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 下一个引用状态
     * @return
     * @throws Exception
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
