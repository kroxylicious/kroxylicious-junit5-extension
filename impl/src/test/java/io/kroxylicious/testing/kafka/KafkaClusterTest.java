/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.ConstraintsMethodSource;
import io.kroxylicious.testing.kafka.common.DimensionMethodSource;
import io.kroxylicious.testing.kafka.common.KRaftCluster;
import io.kroxylicious.testing.kafka.common.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.common.SaslPlainAuth;
import io.kroxylicious.testing.kafka.common.Tls;
import io.kroxylicious.testing.kafka.common.Version;
import io.kroxylicious.testing.kafka.common.ZooKeeperCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static io.kroxylicious.testing.kafka.common.ConstraintUtils.brokerCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.kraftCluster;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.version;
import static io.kroxylicious.testing.kafka.common.ConstraintUtils.zooKeeperCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Test case that simply exercises the ability to control the kafka cluster from the test.
 * It intentional that this test does not involve the proxy.
 */
public class KafkaClusterTest {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterTest.class.getName());
    private static final Duration CLUSTER_FORMATION_TIMEOUT = Duration.ofSeconds(10);
    private TestInfo testInfo;
    private KeytoolCertificateGenerator keytoolCertificateGenerator;
    private static int clusterSizesIndex = 0;

    private static Stream<Version> versions() {
        return Stream.of(
                version("latest")
        // TODO: waiting for new versions support in ozangunalp repo https://github.com/ozangunalp/kafka-native/issues/21
        // version("3.3.1"),
        // version("3.2.1")
        );
    }

    private static Stream<BrokerCluster> clusterSizes() {
        return Stream.of(
                brokerCluster(1),
                brokerCluster(2));
    }

    private final Object[] brokerClusters = clusterSizes().toArray();

    static Stream<List<Annotation>> tuples() {
        return Stream.of(
                List.of(brokerCluster(1), kraftCluster(1)),
                List.of(brokerCluster(2), kraftCluster(1)),
                List.of(brokerCluster(1), zooKeeperCluster()),
                List.of(brokerCluster(2), zooKeeperCluster()));
    }

    @BeforeEach
    public void initializeIndexes() {
        if (clusterSizesIndex == clusterSizes().count()) {
            clusterSizesIndex = 0;
        }
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterTemplate(@ConstraintsMethodSource("tuples") InVMKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerKraftModeTemplate(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @KRaftCluster TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerZookeeperModeTemplate(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @ZooKeeperCluster TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterWithAuth(@ConstraintsMethodSource("tuples") @SaslPlainAuth({
            @SaslPlainAuth.UserPassword(user = "guest", password = "guest")
    }) InVMKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterSASL_SSL(@ConstraintsMethodSource("tuples") @Tls @SaslPlainAuth({
            @SaslPlainAuth.UserPassword(user = "guest", password = "guest")
    }) InVMKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterSSL(@ConstraintsMethodSource("tuples") @Tls InVMKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerKraftModeWithAuth(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @KRaftCluster @SaslPlainAuth({
            @SaslPlainAuth.UserPassword(user = "guest", password = "guest")
    }) TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerKraftModeSASL_SSL(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @KRaftCluster @Tls @SaslPlainAuth({
            @SaslPlainAuth.UserPassword(user = "guest", password = "guest")
    }) TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerKraftModeSSL(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @KRaftCluster @Tls TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerZookeeperModeWithAuth(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @ZooKeeperCluster @SaslPlainAuth({
            @SaslPlainAuth.UserPassword(user = "guest", password = "guest")
    }) TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerZookeeperModeSASL_SSL(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @ZooKeeperCluster @Tls @SaslPlainAuth({
            @SaslPlainAuth.UserPassword(user = "guest", password = "guest")
    }) TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    @TestTemplate
    @ExtendWith(KafkaClusterExtension.class)
    public void kafkaClusterContainerZookeeperModeSSL(@DimensionMethodSource("versions") @DimensionMethodSource("clusterSizes") @ZooKeeperCluster @Tls TestcontainersKafkaCluster cluster)
            throws Exception {
        verifyRecordRoundTrip(((BrokerCluster) brokerClusters[clusterSizesIndex++]).numBrokers(), cluster);
    }

    private void verifyRecordRoundTrip(int expected, KafkaCluster cluster) throws Exception {
        var topic = "TOPIC_1";
        var message = "Hello, world!";

        try (var admin = KafkaAdminClient.create(cluster.getKafkaClientConfiguration())) {
            await().atMost(CLUSTER_FORMATION_TIMEOUT).untilAsserted(() -> assertEquals(expected, getActualNumberOfBrokers(admin)));
            var rf = (short) Math.min(1, Math.max(expected, 3));
            createTopic(admin, topic, rf);
        }

        produce(cluster, topic, message);
        consume(cluster, topic, "Hello, world!");
    }

    private void produce(KafkaCluster cluster, String topic, String message) throws Exception {
        Map<String, Object> config = cluster.getKafkaClientConfiguration();
        config.putAll(Map.<String, Object> of(
                ProducerConfig.CLIENT_ID_CONFIG, "myclient",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        try (var producer = new KafkaProducer<String, String>(config)) {
            producer.send(new ProducerRecord<>(topic, "my-key", message)).get();
        }
    }

    private void consume(KafkaCluster cluster, String topic, String message) throws Exception {
        Map<String, Object> config = cluster.getKafkaClientConfiguration();
        config.putAll(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(Set.of(topic));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            assertEquals(message, records.iterator().next().value());
            consumer.unsubscribe();
        }
    }

    private int getActualNumberOfBrokers(AdminClient admin) throws Exception {
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        return describeClusterResult.nodes().get().size();
    }

    private void createTopic(AdminClient admin1, String topic, short replicationFactor) throws Exception {
        admin1.createTopics(List.of(new NewTopic(topic, 1, replicationFactor))).all().get();
    }

    @AfterAll
    public static void cleanUp() throws IOException {
        File deleteDirectory = new File("/tmp");
        File[] files = deleteDirectory.listFiles((dir, name) -> name.matches("kproxy.*?"));
        assert files != null;
        for (File file : files) {
            FileUtils.deleteDirectory(file);
        }
    }
}
