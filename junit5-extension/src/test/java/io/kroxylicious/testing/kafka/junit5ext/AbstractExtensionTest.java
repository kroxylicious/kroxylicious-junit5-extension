/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractExtensionTest {
    protected DescribeClusterResult describeCluster(Map<String, Object> adminConfig) throws InterruptedException, ExecutionException {
        try (var admin = Admin.create(adminConfig)) {
            return describeCluster(admin);
        }
    }

    @NotNull
    protected static DescribeClusterResult describeCluster(Admin admin) throws InterruptedException, ExecutionException {
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        describeClusterResult.controller().get();
        return describeClusterResult;
    }

    /**
     * Assert that the given admin is connected to a cluster with the same id as the given cluster.
     */
    static DescribeClusterResult assertSameCluster(KafkaCluster cluster, Admin admin) throws ExecutionException, InterruptedException {
        DescribeClusterResult dcr = describeCluster(admin);
        assertEquals(cluster.getClusterId(), dcr.clusterId().get());
        return dcr;
    }

    protected void doConsumer(Consumer<String, String> consumer) {

    }

    protected <K, V> void doProducer(Producer<K, V> producer, K key, V value) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>("my-topic", key, value)).get();
    }
}
