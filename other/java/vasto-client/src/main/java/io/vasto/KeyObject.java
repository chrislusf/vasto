package io.vasto;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class KeyObject {
    private ByteString key;
    private long partitionHash;

    public KeyObject(byte[] key) {
        this.key = ByteString.copyFrom(key);
        if (key != null) {
            this.partitionHash = Hash.hash(key);
        }
    }

    public KeyObject(ByteBuffer key) {
        this.key = ByteString.copyFrom(key);
        if (key != null) {
            this.partitionHash = Hash.hash(key);
        }
    }

    public KeyObject setPartitionKey(byte[] partitionKey) {
        if (partitionKey != null) {
            this.partitionHash = Hash.hash(partitionKey);
        }
        return this;
    }

    public KeyObject setPartitionKey(ByteBuffer partitionKey) {
        if (partitionKey != null) {
            this.partitionHash = Hash.hash(partitionKey);
        }
        return this;
    }

    public KeyObject setPartitionHash(long partitionHash) {
        this.partitionHash = partitionHash;
        return this;
    }

    public ByteString getKey() {
        return key;
    }

    public long getPartitionHash() {
        return partitionHash;
    }
}
