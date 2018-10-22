package io.vasto;

public class PartitionedRequest {
    private Request.Builder reqeustBuilder;
    private long partitionHash;

    public PartitionedRequest(long partitionHash, Request.Builder reqeustBuilder) {
        this.reqeustBuilder = reqeustBuilder;
        this.partitionHash = partitionHash;
    }

    public Request.Builder getReqeustBuilder() {
        return reqeustBuilder;
    }

    public long getPartitionHash() {
        return partitionHash;
    }

}
