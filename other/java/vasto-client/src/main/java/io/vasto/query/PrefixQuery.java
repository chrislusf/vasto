package io.vasto.query;

import com.google.protobuf.ByteString;
import io.vasto.Hash;

import java.nio.ByteBuffer;

public class PrefixQuery {
    private ByteString prefix;
    private long partitionHash;
    private ByteString lastSeenKey;
    private int limit;

    public PrefixQuery(ByteBuffer prefix, long partitionHash, ByteBuffer lastSeenKey, int limit) {
        this.prefix = ByteString.copyFrom(prefix);
        this.partitionHash = partitionHash;
        this.lastSeenKey = ByteString.copyFrom(lastSeenKey);
        this.limit = limit;
    }

    public PrefixQuery(byte[] prefix, long partitionHash, byte[] lastSeenKey, int limit) {
        this.prefix = ByteString.copyFrom(prefix);
        this.partitionHash = partitionHash;
        this.lastSeenKey = ByteString.copyFrom(lastSeenKey);
        this.limit = limit;
    }

    public ByteString getPrefix() {
        return prefix;
    }

    public PrefixQuery setPrefix(ByteBuffer prefix) {
        this.prefix = ByteString.copyFrom(prefix);
        return this;
    }

    public PrefixQuery setPrefix(byte[] prefix) {
        this.prefix = ByteString.copyFrom(prefix);
        return this;
    }

    public long getPartitionHash() {
        return partitionHash;
    }

    public PrefixQuery setPartitionKey(byte[] partitionKey) {
        if (partitionKey != null) {
            this.partitionHash = Hash.hash(partitionKey);
        }
        return this;
    }

    public PrefixQuery setPartitionKey(ByteBuffer partitionKey) {
        if (partitionKey != null) {
            this.partitionHash = Hash.hash(partitionKey);
        }
        return this;
    }

    public PrefixQuery setPartitionHash(long partitionHash) {
        this.partitionHash = partitionHash;
        return this;
    }

    public ByteString getLastSeenKey() {
        return lastSeenKey;
    }

    public PrefixQuery setLastSeenKey(ByteString lastSeenKey) {
        this.lastSeenKey = lastSeenKey;
        return this;
    }

    public PrefixQuery setLastSeenKey(ByteBuffer lastSeenKey) {
        this.lastSeenKey = ByteString.copyFrom(lastSeenKey);
        return this;
    }

    public PrefixQuery setLastSeenKey(byte[] lastSeenKey) {
        this.lastSeenKey = ByteString.copyFrom(lastSeenKey);
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public PrefixQuery setLimit(int limit) {
        this.limit = limit;
        return this;
    }
}
