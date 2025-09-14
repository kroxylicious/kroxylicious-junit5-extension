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
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.OPTIONAL;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(KafkaClusterExtension.class)
class StaticFieldExtensionTest extends AbstractExtensionTest {

    public static final String FIXED_TOPIC_NAME = "fixed";
    public static final String CUSTOM_TOPIC_NAME = "customTopic";
    @Order(1)
    @BrokerCluster(numBrokers = 1)
    static KafkaCluster staticCluster;

    @Order(2)
    static Admin staticAdmin;

    @Order(3)
    static AdminClient staticAdminClient;

    static Topic staticTopic;

    @TopicNameMethodSource
    static Topic customNamedTopic;

    @SuppressWarnings("unused") // used via @TopicNameMethodSource
    static String topicName() {
        return FIXED_TOPIC_NAME;
    }

    @TopicNameMethodSource("customTopicName")
    static Topic customNamedTopicByNamedInstanceMethod;

    @TopicNameMethodSource(clazz = AbstractExtensionTest.class, value = "anotherCustomTopicName")
    static Topic customNamedTopicFromAnotherClass;

    @SuppressWarnings("unused") // used via @TopicNameMethodSource
    static String customTopicName() {
        return CUSTOM_TOPIC_NAME;
    }

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
    void topicStaticField() {
        ObjectAssert<Topic> topicAssert = assertThat(staticTopic)
                .isNotNull();
        topicAssert.extracting(Topic::name).isNotNull();
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();
    }

    @Test
    void topicStaticFieldWithCustomName() {
        ObjectAssert<Topic> topicAssert = assertThat(customNamedTopic)
                .isNotNull();
        topicAssert.extracting(Topic::name, STRING).isNotNull().isEqualTo(FIXED_TOPIC_NAME);
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();
    }

    @Test
    void topicCustomNamedTopicByNamedMethod() {
        ObjectAssert<Topic> topicAssert = assertThat(customNamedTopicByNamedInstanceMethod)
                .isNotNull();
        topicAssert.extracting(Topic::name, STRING).isNotNull().isEqualTo(CUSTOM_TOPIC_NAME);
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();
    }

    @Test
    void topicCustomNamedTopicFromAnotherClass() {
        ObjectAssert<Topic> topicAssert = assertThat(customNamedTopicFromAnotherClass)
                .isNotNull();
        topicAssert.extracting(Topic::name, STRING).isNotNull().isEqualTo(ANOTHER_FIXED_TOPIC_NAME);
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();
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
