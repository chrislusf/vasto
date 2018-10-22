package io.vasto;


import com.github.ssedano.hash.JumpConsistentHash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

public class ClusterTopology {
    private static final Logger logger = Logger.getLogger(ClusterTopology.class.getName());

    private String keyspace;
    private List<LogicShardGroup> logicalShards;
    private int expectedSize;
    private int replicationFactor;
    private ClusterTopology nextCluster;

    public ClusterTopology(String keyspace, int expectedSize, int replicationFactor) {
        this.keyspace = keyspace;
        this.expectedSize = expectedSize;
        this.replicationFactor = replicationFactor;
        this.logicalShards = new ArrayList<>();
    }

    public int findShardId(long keyHash) {
        return JumpConsistentHash.jumpConsistentHash(keyHash, this.expectedSize);
    }

    public StoreResource getStoreResourceByShardId(int shardId) {
        if (this.logicalShards.size() <= shardId) {
            return null;
        }
        LogicShardGroup logicShardGroup = this.logicalShards.get(shardId);
        if (logicShardGroup.clusterNodes.size() <= 0) {
            return null;
        }
        return logicShardGroup.clusterNodes.get(0).getStoreResource();
    }

    private void ensureLogicalShardsCapacity(int shardId) {
        while (this.logicalShards.size() <= shardId) {
            this.logicalShards.add(new LogicShardGroup());
        }
    }

    public void setShard(StoreResource storeResource, ShardInfo shardInfo) {
        int shardId = shardInfo.getShardId();
        this.ensureLogicalShardsCapacity(shardId);
        LogicShardGroup shardGroup = this.logicalShards.get(shardId);

        for (LogicShard node : shardGroup.clusterNodes) {
            if (node.getStoreResource().getAddress().equals(storeResource.getAddress())
                && node.getShardInfo().getShardId() == shardId) {
                node.setShardInfo(shardInfo);
                return;
            }
        }

        shardGroup.add(storeResource, shardInfo);

        if (this.expectedSize != shardInfo.getClusterSize()) {
            this.expectedSize = shardInfo.getClusterSize();
        }
        if (this.replicationFactor != shardInfo.getReplicationFactor()) {
            this.replicationFactor = shardInfo.getReplicationFactor();
        }

    }

    // ReplaceShard ReplaceShard the shardInfo on the server in the cluster.
    // It returns true if the operation is successful.
    public boolean replaceShard(StoreResource storeResource, ShardInfo shardInfo) {
        int shardId = shardInfo.getShardId();
        this.ensureLogicalShardsCapacity(shardId);
        LogicShardGroup shardGroup = this.logicalShards.get(shardId);

        for (LogicShard node : shardGroup.clusterNodes) {
            if (node.getStoreResource().getAddress().equals(storeResource.getAddress())
                && node.getShardInfo().getShardId() == shardId) {
                node.setShardInfo(shardInfo);
                node.setStoreResource(storeResource);
                return true;
            }
        }

        logger.severe(String.format("replace shard: shard %s not found", shardInfo));

        return false;
    }

    // removeShard return true if the whole store has been deleted
    public boolean removeShard(StoreResource storeResource, ShardInfo shardInfo) {
        int shardId = shardInfo.getShardId();
        if (this.logicalShards.size() <= shardId) {
            return false;
        }
        //TODO
        return true;
    }

    public int getCurrentSize() {
        for (int i = this.logicalShards.size(); i > 0; i--) {
            if (this.logicalShards.get(i - 1).clusterNodes.size() == 0) {
                continue;
            }
            return i;
        }
        return 0;
    }

    public void setNextCluster(int expectedSize, int replicationFactor) {
        this.nextCluster = new ClusterTopology(this.getKeyspace(), expectedSize, replicationFactor);
    }

    public void removeNextCluster() {
        this.nextCluster = null;
    }

    // getter and setter
    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public void setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public ClusterTopology getNextCluster() {
        return nextCluster;
    }

    public void setNextCluster(ClusterTopology nextCluster) {
        this.nextCluster = nextCluster;
    }

    private static class LogicShard {
        private StoreResource storeResource;
        private ShardInfo shardInfo;

        public LogicShard(StoreResource storeResource, ShardInfo shardInfo) {
            this.storeResource = storeResource;
            this.shardInfo = shardInfo;
        }

        public StoreResource getStoreResource() {
            return storeResource;
        }

        public void setStoreResource(StoreResource storeResource) {
            this.storeResource = storeResource;
        }

        public ShardInfo getShardInfo() {
            return shardInfo;
        }

        public void setShardInfo(ShardInfo shardInfo) {
            this.shardInfo = shardInfo;
        }
    }

    private static class LogicShardGroup {
        private List<LogicShard> clusterNodes;

        LogicShardGroup() {
            clusterNodes = new ArrayList<>();
        }

        void add(StoreResource storeResource, ShardInfo shardInfo) {
            clusterNodes.add(new LogicShard(storeResource, shardInfo));

            Collections.sort(clusterNodes, new Comparator<LogicShard>() {
                @Override
                public int compare(LogicShard a, LogicShard b) {
                    int x = a.getShardInfo().getServerId() - a.getShardInfo().getShardId();
                    if (x < 0) {
                        x += a.getShardInfo().getClusterSize();
                    }
                    int y = b.getShardInfo().getServerId() - b.getShardInfo().getShardId();
                    if (y < 0) {
                        y += b.getShardInfo().getClusterSize();
                    }
                    return x - y;
                }
            });
        }
    }
}
