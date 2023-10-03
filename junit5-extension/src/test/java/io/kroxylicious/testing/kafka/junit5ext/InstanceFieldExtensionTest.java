/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import com.example.RuntimeMarkerAnnotation;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(KafkaClusterExtension.class)
class InstanceFieldExtensionTest extends AbstractExtensionTest {

    @BrokerCluster(numBrokers = 1)
    KafkaCluster instanceCluster;

    @BrokerCluster(numBrokers = 1)
    @Name("kafkaCluster")
    KafkaCluster namedCluster;

    Consumer<String, String> injectedConsumer;

    Admin injectedAdmin;

    private Admin privateField;

    Producer<String, String> injectedProducer;

    @Order(1)
    Producer<String, String> fieldWithOrderAnnotation;

    final MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    @RuntimeMarkerAnnotation
    Admin fieldWithUnrecognizedAnnotation;

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    Admin fieldWithJavaLangAnnotations;

    @Name("kafkaCluster")
    Admin namedAdmin;

    @Test
    void shouldInjectConsumerField() {
        assertThat(injectedConsumer).isNotNull().isInstanceOf(Consumer.class);
    }

    @Test
    void shouldInjectProducerField() {
        assertThat(injectedProducer).isNotNull().isInstanceOf(Producer.class);
    }

    @Test
    void shouldInjectAdminField() {
        assertThat(injectedAdmin).isNotNull().isInstanceOf(Admin.class);
    }

    @Test
    void shouldInjectIntoPrivateField() {
        assertThat(privateField).isNotNull().isInstanceOf(Admin.class);
    }

    @Test
    void shouldNotInjectIntoInitialisedField() {
        assertThat(mockConsumer).isNotNull().isInstanceOf(MockConsumer.class);
    }

    @Test
    void shouldInjectIntoFieldsWithRecognisedAnnotation() {
        assertThat(fieldWithOrderAnnotation).isNotNull().isInstanceOf(Producer.class);
        assertThat(fieldWithJavaLangAnnotations).isNotNull().isInstanceOf(Admin.class);
        assertThat(namedAdmin).isNotNull().isInstanceOf(Admin.class);
    }

    @Test
    void shouldInjectIntoFieldWithJunitAnnotation() {
        assertThat(fieldWithOrderAnnotation).isNotNull().isInstanceOf(Producer.class);
        // TODO should we use the producer to send a record?
    }

    @Test
    void fieldWithUnrecognizedAnnotationsNotInjected() {
        assertThat(fieldWithUnrecognizedAnnotation).isNull();
    }

    @Test
    void clusterInstanceField()
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(instanceCluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertEquals(instanceCluster.getClusterId(), dc.clusterId().get());
        assertInstanceOf(InVMKafkaCluster.class, instanceCluster);
    }

    @Test
    void adminParameter(Admin admin) throws ExecutionException, InterruptedException {
        assertSameCluster(instanceCluster, admin);
    }
}
