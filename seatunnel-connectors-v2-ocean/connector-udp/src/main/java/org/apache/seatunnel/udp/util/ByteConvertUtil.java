package org.apache.seatunnel.udp.util;

import cn.hutool.core.util.ByteUtil;
import cn.hutool.core.util.HexUtil;

import java.nio.ByteOrder;

public class ByteConvertUtil {
    /** 2字节类型 */
    public static final String SHORT = "SHORT";
    /** 4字节类型 */
    public static final String INT = "INT";

    /**
     * 使用案例
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(parse("c6c6"));
        System.out.println(parse("3AB6231B"));
        String text =
                "C6C64200000000003AB6231B000000000F003D00C21F0000A51C8402421E00000E09000058030000CA1F0000A51C8402E01C27000000000026000A000200451A0100";
    }

    public static int parse(String message) {
        if (message.length() == 4) {
            return ByteUtil.bytesToShort(HexUtil.decodeHex(message), ByteOrder.LITTLE_ENDIAN);
        } else {
            return ByteUtil.bytesToInt(HexUtil.decodeHex(message), ByteOrder.LITTLE_ENDIAN);
        }
    }

    public static String bytesToHexString(byte[] src, Integer byteLength) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < byteLength; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }
}
