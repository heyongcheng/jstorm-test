package cn.sumpay.jstorm.test.utils;

import java.nio.ByteBuffer;

/**
 * @author heyc
 * @date 2018/8/22 17:25
 */
public class Utils {

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

}
