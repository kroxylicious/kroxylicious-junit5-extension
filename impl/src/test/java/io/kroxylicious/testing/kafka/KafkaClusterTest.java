/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaClusterExecutionMode;
import io.kroxylicious.testing.kafka.common.KafkaClusterFactory;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.common.MetadataMode;
import io.kroxylicious.testing.kafka.common.Utils;

import static io.kroxylicious.testing.kafka.common.KafkaClusterFactory.TEST_CLUSTER_EXECUTION_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test case that simply exercises the ability to control the kafka cluster from the test.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class KafkaClusterTest {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterTest.class.getName());
    private TestInfo testInfo;
    private KeytoolCertificateGenerator brokerKeytoolCertificateGenerator;
    private KeytoolCertificateGenerator clientKeytoolCertificateGenerator;

    @Test
    void kafkaClusterKraftMode() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    @EnabledIf(value = "canTestAdditionalControllers", disabledReason = "Test can only pass in IN_VM execution mode")
    void kafkaClusterKraftModeWithMultipleControllers() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .kraftControllers(3)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @SuppressWarnings("unused") // it is used via reflection by junit
    public static boolean canTestAdditionalControllers() {
        final KafkaClusterExecutionMode kafkaClusterExecutionMode = KafkaClusterExecutionMode.convertClusterExecutionMode(
                System.getenv().get(TEST_CLUSTER_EXECUTION_MODE), KafkaClusterExecutionMode.IN_VM);
        return KafkaClusterExecutionMode.CONTAINER != kafkaClusterExecutionMode;
    }

    @Test
    void kafkaClusterZookeeperMode() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class)
    void kafkaClusterAddBroker(MetadataMode metadataMode) throws Exception {
        int brokersNum = 1;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(metadataMode)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getBootstrapServers().split(",")).hasSize(brokersNum);

            verifyRecordRoundTrip(brokersNum, cluster);

            int nodeId = cluster.addBroker();
            assertThat(nodeId).isEqualTo(metadataMode == MetadataMode.KRAFT_SEPARATE ? 2 : 1);
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum + 1);
            assertThat(cluster.getBootstrapServers().split(",")).hasSize(brokersNum + 1);
            verifyRecordRoundTrip(brokersNum + 1, cluster);
        }
    }

    public static Stream<Arguments> stopAndStartBrokers() {
        return Stream.of(
                Arguments.of(1, MetadataMode.KRAFT_COMBINED, TerminationStyle.ABRUPT, (IntPredicate) node -> true),
                Arguments.of(2, MetadataMode.KRAFT_COMBINED, TerminationStyle.ABRUPT, (IntPredicate) node -> node == 1),
                Arguments.of(2, MetadataMode.KRAFT_COMBINED, TerminationStyle.ABRUPT, (IntPredicate) node -> true),
                Arguments.of(1, MetadataMode.KRAFT_COMBINED, TerminationStyle.GRACEFUL, (IntPredicate) node -> true),
                Arguments.of(1, MetadataMode.KRAFT_SEPARATE, TerminationStyle.ABRUPT, (IntPredicate) node -> node >= 1),
                Arguments.of(2, MetadataMode.KRAFT_SEPARATE, TerminationStyle.ABRUPT, (IntPredicate) node -> node == 1),
                Arguments.of(2, MetadataMode.KRAFT_SEPARATE, TerminationStyle.ABRUPT, (IntPredicate) node -> node >= 1),
                Arguments.of(1, MetadataMode.KRAFT_SEPARATE, TerminationStyle.GRACEFUL, (IntPredicate) node -> node >= 1),
                Arguments.of(1, MetadataMode.ZOOKEEPER, TerminationStyle.ABRUPT, (IntPredicate) node -> true));
    }

    @ParameterizedTest
    @MethodSource
    void stopAndStartBrokers(int brokersNum,
                             MetadataMode metadataMode,
                             TerminationStyle terminationStyle,
                             IntPredicate stopBrokerPredicate)
            throws Exception {

        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(metadataMode)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            verifyRecordRoundTrip(brokersNum, cluster);

            var nodes = describeClusterNodes(cluster);
            var brokersExpectedDown = nodes.stream().filter(n -> stopBrokerPredicate.test(n.id())).toList();

            assertThat(cluster.getStoppedBrokers()).hasSize(0);

            cluster.stopNodes(stopBrokerPredicate, terminationStyle);

            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getStoppedBrokers()).hasSameSizeAs(brokersExpectedDown);
            brokersExpectedDown.forEach(this::assertBrokerRefusesConnection);

            cluster.startNodes(stopBrokerPredicate);

            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getStoppedBrokers()).hasSize(0);

            // ensures that all brokers of the cluster are back in service.
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class)
    void stopAndStartIdempotency(MetadataMode metadataMode) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(metadataMode)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);

            assertThat(cluster.getStoppedBrokers()).hasSize(0);
            // starting idempotent
            cluster.startNodes((u) -> true);
            assertThat(cluster.getStoppedBrokers()).hasSize(0);

            cluster.stopNodes((u) -> true, null);
            if (metadataMode == MetadataMode.KRAFT_SEPARATE) {
                assertThat(cluster.getStoppedBrokers()).hasSize(2);
            }
            else {
                assertThat(cluster.getStoppedBrokers()).hasSize(1);
            }

            // stopping idempotent
            cluster.stopNodes((u) -> true, null);
            if (metadataMode == MetadataMode.KRAFT_SEPARATE) {
                assertThat(cluster.getStoppedBrokers()).hasSize(2);
            }
            else {
                assertThat(cluster.getStoppedBrokers()).hasSize(1);
            }

            cluster.startNodes((u) -> true);
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);
            assertThat(cluster.getStoppedBrokers()).hasSize(0);

            // starting idempotent
            cluster.startNodes((u) -> true);
            assertThat(cluster.getStoppedBrokers()).hasSize(0);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class)
    void stopAndStartIncrementally(MetadataMode metadataMode) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(metadataMode)
                .brokersNum(2)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);
            assertThat(cluster.getStoppedBrokers()).hasSize(0);

            cluster.stopNodes((n) -> n == 1, null);
            assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(1);

            cluster.stopNodes((u) -> true, null);
            if (metadataMode == MetadataMode.KRAFT_SEPARATE) {
                assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(0, 1, 2);
            }
            else {
                assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(0, 1);
            }

            // restart one node (in the kraft case, this needs to be the controller).
            cluster.startNodes((n) -> n == 0);
            if (metadataMode == MetadataMode.KRAFT_SEPARATE) {
                assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(1, 2);
            }
            else {
                assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(1);
            }

            cluster.startNodes((u) -> true);
            assertThat(cluster.getStoppedBrokers()).hasSize(0);
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class)
    void topicPersistsThroughStopAndStart(MetadataMode metadataMode) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(metadataMode)
                .brokersNum(1)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();

            var topic = "roundTrip" + Uuid.randomUuid();
            var message = "Hello, world " + Uuid.randomUuid();
            short min = (short) Math.min(cluster.getNumOfBrokers(), 3);

            try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
                createTopic(admin, topic, min);

                produce(cluster, topic, message);

                cluster.stopNodes((u) -> true, null);
                cluster.startNodes((u) -> true);
                verifyRecordRoundTrip(min, cluster);

                // now consume the message we sent before the stop.
                consume(cluster, topic, message);

                deleteTopic(admin, topic);
            }
        }
    }

    @Test
    void kafkaTwoNodeClusterKraftCombinedMode() throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(MetadataMode.KRAFT_COMBINED)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @Test
    void kafkaTwoNodeClusterZookeeperMode() throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class)
    void kafkaClusterRemoveBroker(MetadataMode metadataMode) throws Exception {
        int brokersNum = 3;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(metadataMode)
                .build())) {
            System.out.println("### Cluster built");
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            System.out.println("### Cluster started");
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getBootstrapServers().split(",")).hasSize(brokersNum);
            verifyRecordRoundTrip(brokersNum, cluster);

            cluster.removeBroker(1);

            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum - 1);
            assertThat(cluster.getBootstrapServers().split(",")).hasSize(brokersNum - 1);
            verifyRecordRoundTrip(brokersNum - 1, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class)
    void kafkaClusterRemoveWithStoppedBrokerDisallowed(MetadataMode metadataMode) throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(metadataMode)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();

            cluster.stopNodes(n -> n == 1, null);

            // Node zero is the controller
            assertThrows(IllegalStateException.class, () -> cluster.removeBroker(1),
                    "Expect attempt to remove a node when the cluster has stopped brokers to be rejected");
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = {
            "KRAFT_COMBINED", "KRAFT_SEPARATE"
    })
    void kafkaClusterKraftCombinedDisallowsControllerRemoval(MetadataMode metadataMode) throws Exception {
        int brokersNum = 1;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .metadataMode(metadataMode)
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();

            // Node zero is the controller
            assertThrows(UnsupportedOperationException.class, () -> cluster.removeBroker(0),
                    "Expect kraft to reject removal of controller");
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = {
            "KRAFT_COMBINED", "KRAFT_SEPARATE"
    })
    void kafkaClusterKraftModeWithAuth(MetadataMode metadataMode) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .metadataMode(metadataMode)
                .testInfo(testInfo)
                .securityProtocol("SASL_PLAINTEXT")
                .saslMechanism("PLAIN")
                .user("guest", "pass")
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kafkaClusterZookeeperModeWithAuth() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .securityProtocol("SASL_PLAINTEXT")
                .saslMechanism("PLAIN")
                .user("guest", "pass")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = {
            "KRAFT_COMBINED", "KRAFT_SEPARATE"
    })
    void kafkaClusterKraftModeSASL_SSL_ClientUsesSSLClientAuth(MetadataMode metadataMode) throws Exception {
        createClientCertificate();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .clientKeytoolCertificateGenerator(clientKeytoolCertificateGenerator)
                .metadataMode(metadataMode)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "pass")
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = {
            "KRAFT_COMBINED", "KRAFT_SEPARATE"
    })
    void kafkaClusterKraftModeSSL_ClientUsesSSLClientAuth(MetadataMode metadataMode) throws Exception {
        createClientCertificate();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .clientKeytoolCertificateGenerator(clientKeytoolCertificateGenerator)
                .metadataMode(metadataMode)
                .securityProtocol("SSL")
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kafkaClusterZookeeperModeSASL_SSL_ClientUsesSSLClientAuth() throws Exception {
        createClientCertificate();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .clientKeytoolCertificateGenerator(clientKeytoolCertificateGenerator)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "pass")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kafkaClusterZookeeperModeSSL_ClientUsesSSLClientAuth() throws Exception {
        createClientCertificate();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .clientKeytoolCertificateGenerator(clientKeytoolCertificateGenerator)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = {
            "KRAFT_COMBINED", "KRAFT_SEPARATE"
    })
    void kafkaClusterKraftModeSSL_ClientNoAuth(MetadataMode metadataMode) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .metadataMode(metadataMode)
                .securityProtocol("SSL")
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kafkaClusterZookeeperModeSSL_ClientNoAuth() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataMode.class, names = {
            "KRAFT_COMBINED", "KRAFT_SEPARATE"
    })
    void kafkaClusterKraftModeSASL_SSL_ClientNoAuth(MetadataMode metadataMode) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .metadataMode(metadataMode)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            assumeTrue(metadataMode != MetadataMode.KRAFT_SEPARATE || cluster instanceof InVMKafkaCluster,
                    "Not supported with native image: https://github.com/ozangunalp/kafka-native/issues/88");
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kafkaClusterZookeeperModeSASL_SSL_ClientNoAuth() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .metadataMode(MetadataMode.ZOOKEEPER)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    private void verifyRecordRoundTrip(int expected, KafkaCluster cluster) throws Exception {
        var topic = "roundTrip" + Uuid.randomUuid();
        var message = "Hello, world!";

        try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
            var rf = (short) Math.min(expected, 3);
            createTopic(admin, topic, rf);

            produce(cluster, topic, message);
            consume(cluster, topic, message);

            deleteTopic(admin, topic);
        }

    }

    private void produce(KafkaCluster cluster, String topic, String message) throws Exception {
        Map<String, Object> config = cluster.getKafkaClientConfiguration();
        config.putAll(Map.<String, Object> of(
                ProducerConfig.CLIENT_ID_CONFIG, "myclient",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        try (var producer = CloseableProducer.create(config)) {
            producer.send(new ProducerRecord<>(topic, "my-key", message)).get(30, TimeUnit.SECONDS);
        }
    }

    private void consume(KafkaCluster cluster, String topic, String message) throws Exception {
        Map<String, Object> config = cluster.getKafkaClientConfiguration();
        config.putAll(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (var consumer = CloseableConsumer.create(config)) {
            consumer.subscribe(Set.of(topic));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            assertEquals(message, records.iterator().next().value());
            consumer.unsubscribe();
        }
    }

    private void createTopic(Admin admin, String topic, short replicationFactor) throws Exception {
        Utils.awaitCondition(60, TimeUnit.SECONDS).until(() -> {
            admin.createTopics(List.of(new NewTopic(topic, 1, replicationFactor))).all().get();
            return true;
        });
    }

    private void deleteTopic(Admin admin, String topic) throws Exception {
        admin.deleteTopics(List.of(topic)).all().get();
    }

    private Collection<Node> describeClusterNodes(KafkaCluster cluster) {
        try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
            return Awaitility.waitAtMost(Duration.ofSeconds(10)).until(() -> admin.describeCluster().nodes().get(2, TimeUnit.SECONDS),
                    n -> n.size() == cluster.getNumOfBrokers());
        }
    }

    private void assertBrokerRefusesConnection(Node n) {
        try (var ignored = new Socket(n.host(), n.port())) {
            // If we get this far, the connection has been established, which is unexpected.
            fail("unexpected successful connection open to broker " + n);
        }
        catch (ConnectException e) {
            // pass - this is the expected "connection refused"
        }
        catch (Throwable e) {
            fail("unexpected exception probing for broker " + n, e);
        }
    }

    @BeforeEach
    void before(TestInfo testInfo) throws IOException {
        this.testInfo = testInfo;
        this.brokerKeytoolCertificateGenerator = new KeytoolCertificateGenerator();
    }

    private void createClientCertificate() throws GeneralSecurityException, IOException {
        this.clientKeytoolCertificateGenerator = new KeytoolCertificateGenerator();
        this.clientKeytoolCertificateGenerator.generateSelfSignedCertificateEntry("clientTest@kroxylicious.io", "client", "Dev", "Kroxylicious.ip", null, null, "US");
    }
}
