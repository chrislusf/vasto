package io.vasto.benchmark;

import io.grpc.Context;
import io.vasto.ClusterClient;
import io.vasto.KeyObject;
import io.vasto.OpAndDataType;
import io.vasto.ValueObject;
import io.vasto.VastoClient;
import io.vasto.exception.NoClusterFoundException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

public class VastoBenchmark {
    public static void main(String[] args) throws Exception {

        VastoClient client = new VastoClient(Context.current(), "localhost", 8278, "vs_test");

        ClusterClient ks = client.newClusterClient("benchmark");

        AtomicLongArray counters = new AtomicLongArray(2);
        int concurrency = 10;
        CountDownLatch latch = new CountDownLatch(concurrency);
        int count = 1000000;
        int partitionSize = count / concurrency;

        for (int i = 0; i < concurrency; i++) {
            final int offset = i * partitionSize;
            final int limit = partitionSize;
            new Thread() {
                public void run() {
                    try {
                        partitionedBench(ks, offset, limit, counters);
                    } catch (NoClusterFoundException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            }.start();
        }

        while (!latch.await(1, TimeUnit.SECONDS)) {
            System.out.println("put:" + counters.get(0) + "  get:" + counters.get(1));
        }

        Thread.sleep(1000);

        client.shutdown();

    }

    private static void partitionedBench(ClusterClient clusterClient, int offset, int limit, AtomicLongArray counters) {

        for (int i = offset; i < offset + limit; i++) {
            KeyObject k = new KeyObject(("key" + i).getBytes());
            clusterClient.put(k, new ValueObject(("value" + i).getBytes(), OpAndDataType.BYTES));
            counters.getAndAdd(0, 1);
        }

        for (int i = offset; i < offset + limit; i++) {
            KeyObject k = new KeyObject(("key" + i).getBytes());
            ValueObject valueObject = clusterClient.get(k);
            counters.getAndAdd(1, 1);
            if (valueObject == null) {
                System.out.println("missing value for key: " + new String(k.getKey().toByteArray()));
                continue;
            }
            if (!new String(valueObject.getValue().toByteArray()).equals("value" + i)) {
                System.out.println("value:" + new String(valueObject.getValue().toByteArray()));
            }
        }
    }
}
