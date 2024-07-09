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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.common.MetadataVersion;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import kafka.server.KafkaConfig;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaClusterFactory;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.common.Utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test case that simply exercises the ability to control the kafka cluster from the test.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class KafkaClusterTest {

    private TestInfo testInfo;
    private KeytoolCertificateGenerator brokerKeytoolCertificateGenerator;
    private KeytoolCertificateGenerator clientKeytoolCertificateGenerator;

    @Test
    void zookeeperKafkaCluster() throws Exception {
        kafkaCluster(false);
    }

    @Test
    void kraftKafkaCluster() throws Exception {
        kafkaCluster(true);
    }

    private void kafkaCluster(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(kraft)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kafkaClusterKraftModeWithMultipleControllers() throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(true)
                .kraftControllers(3)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterAddBroker(boolean kraft) throws Exception {
        int brokersNum = 1;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(kraft)
                .build())) {
            cluster.start();
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getBootstrapServers().split(",")).hasSize(brokersNum);

            verifyRecordRoundTrip(brokersNum, cluster);

            int nodeId = cluster.addBroker();
            assertThat(nodeId).isEqualTo(1);
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum + 1);
            assertThat(cluster.getBootstrapServers().split(",")).hasSize(brokersNum + 1);
            verifyRecordRoundTrip(brokersNum + 1, cluster);
        }
    }

    public static Stream<Arguments> stopAndStartBrokers() {
        return Stream.of(
                Arguments.of(1, true, TerminationStyle.ABRUPT, (IntPredicate) node -> true),
                Arguments.of(2, true, TerminationStyle.ABRUPT, (IntPredicate) node -> node == 1),
                Arguments.of(2, true, TerminationStyle.ABRUPT, (IntPredicate) node -> true),
                Arguments.of(1, true, TerminationStyle.GRACEFUL, (IntPredicate) node -> true),
                Arguments.of(1, false, TerminationStyle.ABRUPT, (IntPredicate) node -> true));
    }

    @ParameterizedTest
    @MethodSource
    void stopAndStartBrokers(int brokersNum, boolean kraft, TerminationStyle terminationStyle, IntPredicate brokerPredicate) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(kraft)
                .build())) {
            cluster.start();
            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            verifyRecordRoundTrip(brokersNum, cluster);

            var nodes = describeClusterNodes(cluster);
            var brokersExpectedDown = nodes.stream().filter(n -> brokerPredicate.test(n.id())).toList();

            assertThat(cluster.getStoppedBrokers()).isEmpty();

            cluster.stopNodes(brokerPredicate, terminationStyle);

            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getStoppedBrokers()).hasSameSizeAs(brokersExpectedDown);
            brokersExpectedDown.forEach(this::assertBrokerRefusesConnection);

            cluster.startNodes(brokerPredicate);

            assertThat(cluster.getNumOfBrokers()).isEqualTo(brokersNum);
            assertThat(cluster.getStoppedBrokers()).isEmpty();

            // ensures that all brokers of the cluster are back in service.
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void stopAndStartIdempotency(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(kraft)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);

            assertThat(cluster.getStoppedBrokers()).isEmpty();
            // starting idempotent
            cluster.startNodes((u) -> true);
            assertThat(cluster.getStoppedBrokers()).isEmpty();

            cluster.stopNodes((u) -> true, null);
            assertThat(cluster.getStoppedBrokers()).hasSize(1);

            // stopping idempotent
            cluster.stopNodes((u) -> true, null);
            assertThat(cluster.getStoppedBrokers()).hasSize(1);

            cluster.startNodes((u) -> true);
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);
            assertThat(cluster.getStoppedBrokers()).isEmpty();

            // starting idempotent
            cluster.startNodes((u) -> true);
            assertThat(cluster.getStoppedBrokers()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void stopAndStartIncrementally(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(kraft)
                .brokersNum(2)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);
            assertThat(cluster.getStoppedBrokers()).isEmpty();

            cluster.stopNodes((n) -> n == 1, null);
            assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(1);

            cluster.stopNodes((u) -> true, null);
            assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(0, 1);

            // restart one node (in the kraft case, this needs to be the controller).
            cluster.startNodes((n) -> n == 0);
            assertThat(cluster.getStoppedBrokers()).containsExactlyInAnyOrder(1);

            cluster.startNodes((u) -> true);
            assertThat(cluster.getStoppedBrokers()).isEmpty();
            verifyRecordRoundTrip(cluster.getNumOfBrokers(), cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void topicPersistsThroughStopAndStart(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(kraft)
                .brokersNum(1)
                .build())) {
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

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaTwoNodeCluster(boolean kraft) throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(kraft)
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(brokersNum, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterRemoveBroker(boolean kraft) throws Exception {
        int brokersNum = 3;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(kraft)
                .build())) {
            cluster.start();
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
    @ValueSource(booleans = { true, false })
    void kafkaClusterRemoveWithStoppedBrokerDisallowed(boolean kraft) throws Exception {
        int brokersNum = 2;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(kraft)
                .build())) {
            cluster.start();

            cluster.stopNodes(n -> n == 1, null);

            // Node zero is the controller
            assertThrows(IllegalStateException.class, () -> cluster.removeBroker(1),
                    "Expect attempt to remove a node when the cluster has stopped brokers to be rejected");
        }
    }

    @Test
    void kafkaClusterKraftDisallowsControllerRemoval() throws Exception {
        int brokersNum = 1;
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokersNum(brokersNum)
                .kraftMode(true)
                .build())) {
            cluster.start();

            // Node zero is the controller
            assertThrows(UnsupportedOperationException.class, () -> cluster.removeBroker(0),
                    "Expect kraft to reject removal of controller");
        }
    }

    static Stream<Arguments> kafkaClusterWithUsernamePasswordBasedSaslAuth() {
        return Stream.of(
                Arguments.of("PLAIN", true),
                Arguments.of("PLAIN", false),
                Arguments.of("SCRAM-SHA-256", true),
                Arguments.of("SCRAM-SHA-512", true),
                Arguments.of("SCRAM-SHA-512", false));
    }

    @ParameterizedTest
    @MethodSource
    void kafkaClusterWithUsernamePasswordBasedSaslAuth(String mechanism, boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .kraftMode(kraft)
                .testInfo(testInfo)
                .securityProtocol("SASL_PLAINTEXT")
                .saslMechanism(mechanism)
                .user("guest", "pass")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    /**
     * Tests the ability of the client to authenticate using OAuth Bearer.
     * <br/>
     * That this test uses Kafka's unsecured OAuth Bearer implementation (which
     * uses unsigned JWT tokens).  Note that Kafka enables this mechanism by default.
     * @param kraft kraft mode
     * @throws Exception exception
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterWithOAuthBearerAuth(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .kraftMode(kraft)
                .testInfo(testInfo)
                .saslMechanism("OAUTHBEARER")
                .securityProtocol("SASL_PLAINTEXT")
                .jaasClientOption("unsecuredLoginStringClaim_sub", "principal")
                .jaasServerOption("unsecuredLoginStringClaim_sub", "principal")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterWithOAuthBearerAuthFail(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .kraftMode(kraft)
                .testInfo(testInfo)
                .saslMechanism("OAUTHBEARER")
                .securityProtocol("SASL_PLAINTEXT")
                .jaasClientOption("unsecuredLoginStringClaim_sub", "principal")
                .jaasServerOption("unsecuredLoginStringClaim_sub", "principal")
                .jaasServerOption("unsecuredValidatorRequiredScope", "foo")
                .build())) {
            cluster.start();

            try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
                assertThatThrownBy(() -> performClusterOperation(admin))
                        .hasCauseInstanceOf(SaslAuthenticationException.class)
                        .hasMessageContaining("{\"status\":\"insufficient_scope\", \"scope\":\"[foo]\"}");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterSASL_SSL_ClientUsesSSLClientAuth(boolean kraft) throws Exception {
        createClientCertificate();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .clientKeytoolCertificateGenerator(clientKeytoolCertificateGenerator)
                .kraftMode(kraft)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "pass")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterSSL_ClientUsesSSLClientAuth(boolean kraft) throws Exception {
        createClientCertificate();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .clientKeytoolCertificateGenerator(clientKeytoolCertificateGenerator)
                .kraftMode(kraft)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterSSL_ClientNoAuth(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .kraftMode(kraft)
                .securityProtocol("SSL")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void kafkaClusterSASL_SSL_ClientNoAuth(boolean kraft) throws Exception {
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .brokerKeytoolCertificateGenerator(brokerKeytoolCertificateGenerator)
                .kraftMode(kraft)
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .user("guest", "guest")
                .build())) {
            cluster.start();
            verifyRecordRoundTrip(1, cluster);
        }
    }

    @Test
    void kraftClusterWithMinBootstrapInterBrokerProtocol() throws Exception {
        var minVersion = MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.version();
        try (var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .testInfo(testInfo)
                .kraftMode(true)
                .brokerConfig(KafkaConfig.InterBrokerProtocolVersionProp(), minVersion)
                .build())) {
            cluster.start();

            try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
                var nodeId = "0";
                var cr = new ConfigResource(ConfigResource.Type.BROKER, nodeId);
                var configs = admin.describeConfigs(List.of(cr)).all().get(5, TimeUnit.SECONDS);

                assertThat(configs).containsKey(cr);

                var bcr = configs.get(cr);
                assertThat(bcr).isNotNull();

                var ibp = bcr.get(KafkaConfig.InterBrokerProtocolVersionProp());
                assertThat(ibp)
                        .isNotNull()
                        .extracting(ConfigEntry::value)
                        .asInstanceOf(InstanceOfAssertFactories.STRING)
                        .startsWith(minVersion);
            }
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

    /**
     * Pings the cluster in order to assert connectivity. We don't care about the result.
     * @param admin admin
     */
    @SuppressWarnings("java:S1481") // making clear the intent that the result of the operation is unneeded.
    private void performClusterOperation(Admin admin) {
        var unused = admin.describeCluster().nodes().toCompletionStage().toCompletableFuture().join();
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
