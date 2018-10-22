package io.vasto;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class ValueObject {
    private ByteString value;
    private OpAndDataType dataType;

    public ValueObject(ByteBuffer value, OpAndDataType dataType) {
        this.value = ByteString.copyFrom(value);
        this.dataType = dataType;
    }

    public ValueObject(byte[] value, OpAndDataType dataType) {
        this.value = ByteString.copyFrom(value);
        this.dataType = dataType;
    }

    public ValueObject(ByteString value, OpAndDataType dataType) {
        this.value = value;
        this.dataType = dataType;
    }

    public ByteString getValue() {
        return value;
    }

    public OpAndDataType getDataType() {
        return dataType;
    }
}
