package io.vasto;

import net.openhft.hashing.LongHashFunction;

import java.nio.ByteBuffer;

public class Hash {
    public static long hash(byte[] key) {
        return LongHashFunction.xx().hashBytes(key);
    }

    public static long hash(ByteBuffer key) {
        return LongHashFunction.xx().hashBytes(key);
    }
}
