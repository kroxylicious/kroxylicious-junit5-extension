/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.common.KRaftCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
class BrokerConfigOverrideIntegrationTest {

    private static final String TRANSACTIONAL_ID = "txn-override-test";

    @Test
    void transactionsWorkWithReplicatedTransactionStateTopic(
                                                             @BrokerCluster(numBrokers = 3) @KRaftCluster(numControllers = 3) @BrokerConfig(name = "transaction.state.log.replication.factor", value = "3") @BrokerConfig(name = "transaction.state.log.min.isr", value = "2") KafkaCluster cluster,
                                                             Admin admin,
                                                             @ClientConfig(name = ProducerConfig.TRANSACTIONAL_ID_CONFIG, value = TRANSACTIONAL_ID) Producer<String, String> producer,
                                                             @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "txn-test-group") @ClientConfig(name = ConsumerConfig.ISOLATION_LEVEL_CONFIG, value = "read_committed") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> consumer)
            throws Exception {

        var topic = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topic, "key", "txn-value")).get(10, TimeUnit.SECONDS);
        producer.commitTransaction();

        consumer.subscribe(List.of(topic));
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    var records = consumer.poll(Duration.ofSeconds(1));
                    assertThat(records).isNotEmpty();
                    assertThat(records.iterator().next().value()).isEqualTo("txn-value");
                });
    }
}
