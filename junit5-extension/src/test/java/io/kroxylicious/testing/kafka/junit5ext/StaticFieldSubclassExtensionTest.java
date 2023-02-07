/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(KafkaClusterExtension.class)
public class StaticFieldSubclassExtensionTest extends AbstractExtensionTest {

    /**
     * We can declare the field type using a specific subclass of KafkaCluster
     */
    @BrokerCluster(numBrokers = 1)
    static TestcontainersKafkaCluster staticCluster;

    @Test
    public void testKafkaClusterStaticField()
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(staticCluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertEquals(staticCluster.getClusterId(), dc.clusterId().get());
        assertInstanceOf(TestcontainersKafkaCluster.class, staticCluster);
    }

    @Test
    public void adminParameter(Admin admin) throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, admin);
    }

    @Test
    public void adminClientParameter(AdminClient admin) throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, admin);
    }

    @Test
    public void kafkaAdminClientParameter(KafkaAdminClient admin) throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, admin);
    }

    @Test
    public void producerParameter(Producer<String, String> producer) throws ExecutionException, InterruptedException {
        doProducer(producer, "hello", "world");
    }

    @Test
    public void kafkaProducerParameter(KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        doProducer(producer, "hello", "world");
    }

    @Test
    public void consumerParameter(Consumer<String, String> consumer) {
        doConsumer(consumer);
    }

    @Test
    public void kafkaConsumerParameter(KafkaConsumer<String, String> consumer) {
        doConsumer(consumer);
    }

}
