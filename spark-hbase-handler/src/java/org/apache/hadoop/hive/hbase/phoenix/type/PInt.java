package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PInt {
    public static int decodeInt(byte[] bytes, int o) {
        int v;
        v = bytes[o] ^ 0xff ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
            v = (v << 8) + ((bytes[o + i] ^ 0xff) & 0xff);
        }
        return v;
    }

    public static int decodeColumn(byte[] bytes, int o) {
        int v;
        v = bytes[o] ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
            v = (v << 8) + (bytes[o + i] & 0xff);
        }
        return v;
    }
}
