package io.vasto;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

// ClusterListner is shared by different clusters
public class ClusterListner {
    private static final Logger logger = Logger.getLogger(ClusterListner.class.getName());

    private String clientName;
    private StreamObserver<ClientHeartbeat> clientHeartbeatStreamObserver;
    private ConcurrentMap<String, ClusterTopology> clusters;
    private ConcurrentMap<String, Boolean> justRegisteredClusters;
    private ManagedChannel channel;
    private ConcurrentMap<String, ConnectionPool> connectionPoolConcurrentMap;

    public ClusterListner(String clientName) {
        this.clientName = clientName;
        this.clusters = new ConcurrentHashMap<>();
        this.justRegisteredClusters = new ConcurrentHashMap<>();
        this.connectionPoolConcurrentMap = new ConcurrentHashMap<>();
    }

    public void startListener(Context ctx, String masterHost, int masterPort) {
        try {
            this.registerClientAtMasterServer(masterHost, masterPort);
        } catch (Throwable t) {
            logger.log(Level.WARNING, String.format("client to master %s:%d failed", masterHost, masterPort), t);
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void registerClientAtMasterServer(String masterHost, int masterPort)
        throws InterruptedException {

        channel = ManagedChannelBuilder.forAddress(masterHost, masterPort)
            .usePlaintext(true)
            .build();
        VastoMasterGrpc.VastoMasterStub vastoMasterStub = VastoMasterGrpc.newStub(channel);

        clientHeartbeatStreamObserver = vastoMasterStub.registerClient(
            new StreamObserver<ClientMessage>() {
                @Override
                public void onNext(ClientMessage value) {
                    processClientMessage(value);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            });

    }

    public void registerForClusterAtMaster(String keyspace, boolean isUnfollow, String clientName) {
        if (this.clusters.get(keyspace) == null) {
            justRegisteredClusters.putIfAbsent(keyspace, true);
        }
        this.clientHeartbeatStreamObserver.onNext(ClientHeartbeat.newBuilder()
            .setClientName(clientName)
            .setClusterFollow(ClientHeartbeat.ClusterFollowMessage.newBuilder()
                .setKeyspace(keyspace)
                .setIsUnfollow(isUnfollow)
                .build())
            .build());
    }

    private void processClientMessage(ClientMessage msg) {
        if (msg.hasCluster()) {
            Cluster cluster = msg.getCluster();
            ClusterTopology clusterTopology = getOrSetCluster(cluster.getKeyspace(),
                cluster.getExpectedClusterSize(), cluster.getReplicationFactor());
            for (ClusterNode node : cluster.getNodesList()) {
                addNode(clusterTopology, node);
            }
            justRegisteredClusters.remove(cluster.getKeyspace());
        } else if (msg.hasUpdates()) {
            ClientMessage.StoreResourceUpdate updates = msg.getUpdates();
            ClusterTopology clusterTopology = getCluster(updates.getKeyspace());
            if (clusterTopology == null) {
                logger.severe(String.format("%s no keyspace %s to update", clientName, updates.getKeyspace()));
                return;
            }
            for (ClusterNode node : updates.getNodesList()) {
                if (updates.getIsPromotion()) {
                    promoteNode(clusterTopology, node);
                } else if (updates.getIsDelete()) {
                    removeNode(clusterTopology, node);
                } else {
                    addNode(clusterTopology, node);
                }
            }
        } else if (msg.hasResize()) {
            ClientMessage.Resize resize = msg.getResize();
            ClusterTopology clusterTopology = getCluster(resize.getKeyspace());
            if (clusterTopology == null) {
                logger.severe(String.format("%s no keyspace %s to resize", clientName, resize.getKeyspace()));
                return;
            }
            clusterTopology.setExpectedSize(resize.getTargetClusterSize());
        } else {
            logger.severe(String.format("%s unknown message %s", clientName, msg.toString()));
        }

    }

    public ClusterTopology getCluster(String keyspace) {
        while (justRegisteredClusters.containsKey(keyspace)) {
            try {
                Thread.sleep(30L);
            } catch (InterruptedException e) {
            }
        }
        return this.clusters.get(keyspace);
    }

    private ClusterTopology getOrSetCluster(String keyspace, int clusterSize, int replicationFactor) {
        ClusterTopology cluster = this.clusters.computeIfAbsent(keyspace, new Function<String, ClusterTopology>() {
            @Override
            public ClusterTopology apply(String keyspace) {
                return new ClusterTopology(keyspace, clusterSize, replicationFactor);
            }
        });

        // in case the old cluster has old cluster size
        if (clusterSize > 0) {
            cluster.setExpectedSize(clusterSize);
        }
        if (replicationFactor > 0) {
            cluster.setReplicationFactor(replicationFactor);
        }
        return cluster;
    }

    private void addNode(ClusterTopology clusterTopology, ClusterNode node) {
        if (node.getShardInfo().getIsCandidate()) {
            if (clusterTopology.getNextCluster() == null) {
                clusterTopology.setNextCluster(
                    node.getShardInfo().getClusterSize(), node.getShardInfo().getReplicationFactor());
            }
            clusterTopology = clusterTopology.getNextCluster();
        }

        logger.info(String.format("adding node:\n%s\n%s", node.getStoreResource(), node.getShardInfo()));

        clusterTopology.setShard(node.getStoreResource(), node.getShardInfo());
    }

    private void removeNode(ClusterTopology clusterTopology, ClusterNode node) {
        if (node.getShardInfo().getIsCandidate()) {
            if (clusterTopology.getNextCluster() == null) {
                clusterTopology.setNextCluster(new ClusterTopology(
                    clusterTopology.getKeyspace(),
                    node.getShardInfo().getClusterSize(), node.getShardInfo().getReplicationFactor()));
            }
            clusterTopology = clusterTopology.getNextCluster();
        }

        boolean isStoreDeleted = clusterTopology.removeShard(node.getStoreResource(), node.getShardInfo());
        if (isStoreDeleted) {
            //TODO
        }

    }

    private void promoteNode(ClusterTopology clusterTopology, ClusterNode node) {

        ClusterTopology candidateCluster = clusterTopology.getNextCluster();
        if (candidateCluster == null) {
            return;
        }

        if (node != null) {
            candidateCluster.removeShard(node.getStoreResource(), node.getShardInfo());
            if (candidateCluster.getCurrentSize() == 0) {
                clusterTopology.removeNextCluster();
            }
            if (!clusterTopology.replaceShard(node.getStoreResource(), node.getShardInfo())) {
                clusterTopology.setShard(node.getStoreResource(), node.getShardInfo());
            }
        }

    }

    public Connection getConnection(StoreResource storeResource) {
        if (storeResource == null) {
            return null;
        }

        ConnectionPool pool = this.connectionPoolConcurrentMap.computeIfAbsent(
            storeResource.getAddress(), new Function<String, ConnectionPool>() {
                @Override
                public ConnectionPool apply(String address) {
                    return new ConnectionPool(address, 32, 5000);
                }
            }
        );

        try {
            return pool.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;

    }
}
