/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(KafkaClusterExtension.class)
class StaticFieldExtensionTest extends AbstractExtensionTest {

    @Order(1)
    @BrokerCluster(numBrokers = 1)
    static KafkaCluster staticCluster;

    @Order(2)
    static Admin staticAdmin;

    @Order(3)
    static AdminClient staticAdminClient;

    @Test
    void testKafkaClusterStaticField()
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(staticCluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertEquals(staticCluster.getClusterId(), dc.clusterId().get());
        var cbc = assertInstanceOf(InVMKafkaCluster.class, staticCluster);
    }

    @Test
    void adminStaticField() throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, staticAdmin);
    }

    @Test
    void adminClientStaticField() throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, staticAdminClient);
    }

    @Test
    void adminParameter(Admin admin) throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, admin);
    }

    @Test
    void adminClientParameter(AdminClient admin) throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, admin);
    }

    @Test
    void kafkaAdminClientParameter(KafkaAdminClient admin) throws ExecutionException, InterruptedException {
        assertSameCluster(staticCluster, admin);
    }

    @Test
    void producerParameter(Producer<String, String> producer) throws ExecutionException, InterruptedException {
        doProducer(producer, "hello", "world");
    }

    @Test
    void kafkaProducerParameter(KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        doProducer(producer, "hello", "world");
    }

    @Test
    void consumerParameter(Consumer<String, String> consumer) {
        doConsumer(consumer);
    }

    @Test
    void kafkaConsumerParameter(KafkaConsumer<String, String> consumer) {
        doConsumer(consumer);
    }

}
