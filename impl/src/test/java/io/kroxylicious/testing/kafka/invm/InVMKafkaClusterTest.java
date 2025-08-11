/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;

import static org.assertj.core.api.Assertions.assertThat;

public class InVMKafkaClusterTest {

    @Test
    void scramUsersCreated() {
        KafkaClusterConfig config = KafkaClusterConfig.builder().securityProtocol("SASL_PLAINTEXT").saslMechanism("SCRAM-SHA-256")
                .user("admin", "admin-secret").build();
        try (InVMKafkaCluster cluster = new InVMKafkaCluster(config)) {
            cluster.start();
            Map<String, Object> kafkaClientConfiguration = cluster.getKafkaClientConfiguration();
            assertThat(kafkaClientConfiguration).containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            assertThat(kafkaClientConfiguration).containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            kafkaClientConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            kafkaClientConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            try (var producer = CloseableProducer.create(kafkaClientConfiguration)) {
                Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("topic", "key", "value"));
                RecordMetadata recordMetadata = metadataFuture.get(10, TimeUnit.SECONDS);
                assertThat(recordMetadata).isNotNull();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
