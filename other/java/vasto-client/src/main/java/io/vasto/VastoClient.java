package io.vasto;

import io.grpc.Context;

import java.util.logging.Logger;

/**
 * A Vasto client.
 */
public class VastoClient {
    private static final Logger logger = Logger.getLogger(VastoClient.class.getName());

    private Context ctx;
    private String clientName;
    private ClusterListner clusterListner;

    /**
     * Construct client connecting to Vasto master server.
     */
    public VastoClient(Context ctx, String masterHost, int masterPort, String clientName) {
        this.ctx = ctx;
        this.clientName = clientName;

        this.clusterListner = new ClusterListner(clientName);
        this.clusterListner.startListener(ctx, masterHost, masterPort);
    }

    public ClusterClient newClusterClient(String keyspace) {
        return new ClusterClient(keyspace, this.clusterListner, this.clientName);
    }

    public void shutdown() throws InterruptedException {
        this.clusterListner.shutdown();
    }

}
