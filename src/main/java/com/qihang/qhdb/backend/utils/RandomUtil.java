package com.qihang.qhdb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @Author: zhqihang
 * @Date: 2024/03/12
 * @Project: qhdb
 * @Description: ...
 */
public class RandomUtil {
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
