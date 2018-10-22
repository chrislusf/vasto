package io.vasto;

import com.google.protobuf.InvalidProtocolBufferException;
import io.vasto.exception.NoClusterFoundException;
import io.vasto.query.PrefixQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ClusterClient {
    private static final Logger logger = Logger.getLogger(ClusterClient.class.getName());

    private String keyspace;
    private ClusterListner clusterListner;

    public ClusterClient(String keyspace, ClusterListner clusterListner, String clientName) {
        this.keyspace = keyspace;
        this.clusterListner = clusterListner;
        this.clusterListner.registerForClusterAtMaster(this.keyspace, false, clientName);
    }

    public void put(KeyObject keyObject, ValueObject value) {
        put(keyObject, value, 0, 0);
    }

    public void put(KeyObject keyObject, ValueObject value, long updatedAtNs, int ttlSeconds) {

        Request.Builder request = Request.newBuilder().setPut(
            PutRequest.newBuilder()
                .setKey(keyObject.getKey())
                .setPartitionHash(keyObject.getPartitionHash())
                .setUpdatedAtNs(updatedAtNs)
                .setTtlSecond(ttlSeconds)
                .setOpAndDataType(value.getDataType())
                .setValue(value.getValue())
                .build()
        );

        List<PartitionedRequest> requests = new ArrayList<>();
        requests.add(new PartitionedRequest(keyObject.getPartitionHash(), request));

        this.batchProcess(requests, new ResponsesProcessor() {
            @Override
            public void onResponses(List<Response> responses) {
                return;
            }
        });

    }

    public ValueObject get(KeyObject keyObject) {

        Request.Builder request = Request.newBuilder().setGet(
            GetRequest.newBuilder()
                .setKey(keyObject.getKey())
                .setPartitionHash(keyObject.getPartitionHash())
                .build()
        );

        List<PartitionedRequest> requests = new ArrayList<>();
        requests.add(new PartitionedRequest(keyObject.getPartitionHash(), request));

        final ValueObject[] valueObject = new ValueObject[1];

        this.batchProcess(requests, new ResponsesProcessor() {
            @Override
            public void onResponses(List<Response> responses) {
                if (responses.size() <= 0) {
                    return;
                }
                Response response = responses.get(0);
                GetResponse getResponse = response.getGet();
                if (getResponse.getOk()) {
                    valueObject[0] = new ValueObject(
                        getResponse.getKeyValue().getValue(),
                        getResponse.getKeyValue().getDataType());
                }
                return;
            }
        });

        return valueObject[0];

    }

    public List<KeyTypeValue> prefix(PrefixQuery prefix) {
        Request.Builder request = Request.newBuilder().setGetByPrefix(
            GetByPrefixRequest.newBuilder()
                .setLastSeenKey(prefix.getLastSeenKey())
                .setLimit(prefix.getLimit())
                .setPrefix(prefix.getPrefix())
                .build()
        );

        List<PartitionedRequest> requests = new ArrayList<>();
        requests.add(new PartitionedRequest(prefix.getPartitionHash(), request));

        List<KeyTypeValue> keyValuesList = new ArrayList<>();

        this.batchProcess(requests, new ResponsesProcessor() {
            @Override
            public void onResponses(List<Response> responses) {
                if (responses.size() <= 0) {
                    return;
                }
                Response response = responses.get(0);
                GetByPrefixResponse getResponse = response.getGetByPrefix();
                if (getResponse.getOk()) {
                    keyValuesList.addAll(getResponse.getKeyValuesList());
                }
                return;
            }
        });

        return keyValuesList;
    }

    public void delete(KeyObject keyObject) {

        Request.Builder request = Request.newBuilder().setDelete(
            DeleteRequest.newBuilder()
                .setKey(keyObject.getKey())
                .setPartitionHash(keyObject.getPartitionHash())
                .build()
        );

        List<PartitionedRequest> requests = new ArrayList<>();
        requests.add(new PartitionedRequest(keyObject.getPartitionHash(), request));

        this.batchProcess(requests, new ResponsesProcessor() {
            @Override
            public void onResponses(List<Response> responses) {
                return;
            }
        });

    }

    private void batchProcess(List<PartitionedRequest> requests, ResponsesProcessor processor) {

        ClusterTopology cluster = this.clusterListner.getCluster(this.keyspace);
        if (cluster == null) {
            throw new NoClusterFoundException(this.keyspace);
        }

        Map<Integer, List<Request>> shardIdToRequests = new HashMap<>();
        for (PartitionedRequest request : requests) {
            int shardId = cluster.findShardId(request.getPartitionHash());
            List<Request> shardedRequests = shardIdToRequests.getOrDefault(shardId, new ArrayList<>());
            shardedRequests.add(request.getReqeustBuilder().setShardId(shardId).build());
            shardIdToRequests.put(shardId, shardedRequests);
        }

        mapEachShard(cluster, shardIdToRequests, processor);

    }

    private void mapEachShard(ClusterTopology cluster,
                              Map<Integer, List<Request>> shardIdToRequests,
                              ResponsesProcessor processor) {

        shardIdToRequests.forEach((shardId, requests) -> {

            Requests.Builder batchRequestsBuilder = Requests.newBuilder();
            requests.forEach((request -> batchRequestsBuilder.addRequests(request)));
            batchRequestsBuilder.setKeyspace(this.keyspace);

            try (Connection connection = clusterListner.getConnection(cluster.getStoreResourceByShardId(shardId))) {
                if (connection != null) {

                    byte[] bytes = connection.send(batchRequestsBuilder.build().toByteArray());

                    if (bytes != null) {

                        Responses responses = null;
                        try {
                            responses = Responses.parseFrom(bytes);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        processor.onResponses(responses.getResponsesList());

                    }

                } else {
                    logger.severe("missing connection to shard " + shardId);
                }
            }

        });
    }


    public interface ResponsesProcessor {
        void onResponses(List<Response> responses);
    }

}
