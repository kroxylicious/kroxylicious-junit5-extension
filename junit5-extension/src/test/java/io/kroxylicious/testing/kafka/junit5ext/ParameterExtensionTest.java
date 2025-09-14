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
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.common.KRaftCluster;
import io.kroxylicious.testing.kafka.common.KafkaClusterFactory;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.common.SaslMechanism.Principal;
import io.kroxylicious.testing.kafka.common.Tls;
import io.kroxylicious.testing.kafka.common.ZooKeeperCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.OPTIONAL;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;
import static org.assertj.core.api.InstanceOfAssertFactories.collection;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
class ParameterExtensionTest extends AbstractExtensionTest {

    private static final Duration CLUSTER_FORMATION_TIMEOUT = Duration.ofSeconds(10);
    private static final String CONSUMER_GROUP = "mygroup";
    private static final String TRANSACTIONAL_ID = "mytxn1";
    private static final String CLIENT_ID = "myclientid";
    public static final String FIXED_TOPIC_NAME = "fixed";
    public static final String CUSTOM_TOPIC_NAME = "customTopicName";

    @Test
    void clusterParameter(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        assertThat(cluster).isInstanceOf(InVMKafkaCluster.class);
        assertClusterIdAndSize(cluster, 2);
    }

    @SetEnvironmentVariable(key = KafkaClusterFactory.TEST_CLUSTER_EXECUTION_MODE, value = "CONTAINER")
    @Test
    void clusterParameterPreferenceContainer(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        assertThat(cluster).isInstanceOf(TestcontainersKafkaCluster.class);
        assertClusterIdAndSize(cluster, 2);
    }

    @SetEnvironmentVariable(key = KafkaClusterFactory.TEST_CLUSTER_EXECUTION_MODE, value = "IN_VM")
    @Test
    void clusterParameterPreferenceInVm(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) throws Exception {
        assertThat(cluster).isInstanceOf(InVMKafkaCluster.class);
        assertClusterIdAndSize(cluster, 2);
    }

    @Test
    void brokerConfigs(@BrokerConfig(name = "compression.type", value = "zstd") @BrokerConfig(name = "delete.topic.enable", value = "false") KafkaCluster clusterWithConfigs,
                       Admin admin)
            throws Exception {
        assertSameCluster(clusterWithConfigs, admin);
        var resource = new ConfigResource(ConfigResource.Type.BROKER, "0");

        var configs = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        assertConfigValue(configs, "compression.type", "zstd");
        assertConfigValue(configs, "delete.topic.enable", "false");
    }

    @Test
    void consumerConfiguration(KafkaCluster cluster,
                               Admin admin,
                               @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = CONSUMER_GROUP) Consumer<String, String> consumer)
            throws Exception {

        var topic = createTopic(admin);

        consumer.subscribe(List.of(topic));
        // start the consumer to create the group
        consumer.poll(Duration.ofSeconds(1));

        var groups = admin.listConsumerGroups().all().get(5, TimeUnit.SECONDS);
        assertThat(groups)
                .singleElement()
                .extracting(ConsumerGroupListing::groupId).isEqualTo(CONSUMER_GROUP);
    }

    @Test
    void producerConfiguration(KafkaCluster cluster,
                               Admin admin,
                               @ClientConfig(name = ProducerConfig.TRANSACTIONAL_ID_CONFIG, value = TRANSACTIONAL_ID) Producer<String, String> producer)
            throws Exception {

        var topic = createTopic(admin);

        producer.initTransactions();
        producer.beginTransaction();
        // send a record to start the transaction
        producer.send(new ProducerRecord<>(topic, "hello world")).get(5, TimeUnit.SECONDS);

        var transactions = admin.describeTransactions(List.of(TRANSACTIONAL_ID)).all().get(5, TimeUnit.SECONDS);
        assertThat(transactions)
                .hasSize(1)
                .containsKey(TRANSACTIONAL_ID);
    }

    @Test
    void adminConfiguration(KafkaCluster cluster,
                            @ClientConfig(name = ConsumerConfig.CLIENT_ID_CONFIG, value = CLIENT_ID) Admin admin) {

        assertThat(admin).isNotNull();
        // reflection is the best we can do.
        assertThat(admin).extracting("clientId").isEqualTo(CLIENT_ID);
    }

    @Test
    void clusterAndAdminParameter(@BrokerCluster(numBrokers = 2) KafkaCluster cluster,
                                  Admin admin)
            throws Exception {
        assertSameCluster(cluster, admin);
        assertClusterIdAndSize(cluster, 2);
        assertThat(cluster).isInstanceOf(InVMKafkaCluster.class);
    }

    @Test
    void twoAnonClusterParameter(@BrokerCluster(numBrokers = 1) KafkaCluster cluster1,
                                 @BrokerCluster(numBrokers = 2) KafkaCluster cluster2)
            throws Exception {
        assertThat(cluster1.getClusterId()).isNotEqualTo(cluster2.getClusterId());

        assertClusterIdAndSize(cluster1, 1);
        assertClusterIdAndSize(cluster2, 2);
    }

    @Test
    void twoDefinedClusterParameterAndAdmin(@BrokerCluster(numBrokers = 1) @Name("A") KafkaCluster clusterA,
                                            @BrokerCluster(numBrokers = 2) @Name("B") KafkaCluster clusterB,
                                            @Name("B") Admin adminB,
                                            @Name("A") Admin adminA)
            throws Exception {
        assertSameCluster(clusterA, adminA);
        assertClusterIdAndSize(clusterA, 1);

        assertSameCluster(clusterB, adminB);
        assertClusterIdAndSize(clusterB, 2);
    }

    @Test
    void multipleReferencesToTheSameCluster(@BrokerCluster(numBrokers = 1) @Name("A") KafkaCluster clusterA,
                                            @Name("A") KafkaCluster clusterARef,
                                            @Name("A") Admin adminA)
            throws Exception {
        assertSameCluster(clusterA, adminA);
        assertSameCluster(clusterARef, adminA);
        assertThat(clusterA).isSameAs(clusterARef);
        assertClusterIdAndSize(clusterA, 1);
    }

    // multiple clients connected to the same cluster (e.g. different users)
    @Test
    void clusterParameterAndTwoAdmin(@BrokerCluster(numBrokers = 1) @Name("A") KafkaCluster cluster1,
                                     @Name("A") Admin admin1,
                                     @Name("A") Admin admin2)
            throws Exception {
        assertSameCluster(cluster1, admin1);
        assertSameCluster(cluster1, admin2);
        assertThat(admin1).isNotSameAs(admin2);
    }

    @Test
    @EnabledIf("io.kroxylicious.testing.kafka.junit5ext.AbstractExtensionTest#zookeeperAvailable")
    void zkBasedClusterParameter(@BrokerCluster @ZooKeeperCluster KafkaCluster cluster)
            throws Exception {
        assertClusterSize(cluster, 1);
        assertThat(cluster.getClusterId())
                .withFailMessage("KafkaCluster.getClusterId() should be null for ZK-based clusters")
                .isNull();
    }

    @Test
    void kraftBasedClusterParameter(@BrokerCluster @KRaftCluster KafkaCluster cluster)
            throws Exception {
        assertClusterIdAndSize(cluster, 1);
    }

    @Test
    void shouldDefaultToSaslPlain(@BrokerCluster @SaslMechanism(principals = { @Principal(user = "alice", password = "foo") }) KafkaCluster cluster) {
        doAuthExpectSucceeds(cluster, "alice", "foo");
    }

    @Test
    void saslPlainAuthExplicitMechanism(@BrokerCluster @SaslMechanism(value = "PLAIN", principals = {
            @Principal(user = "alice", password = "foo") }) KafkaCluster cluster) {
        doAuthExpectSucceeds(cluster, "alice", "foo");
    }

    @Test
    void saslScramAuth(@BrokerCluster @SaslMechanism(value = "SCRAM-SHA-256", principals = { @Principal(user = "alice", password = "foo") }) KafkaCluster cluster) {
        doAuthExpectSucceeds(cluster, "alice", "foo");
    }

    @Test
    void saslAuthWithManyUsers(@BrokerCluster @SaslMechanism(value = "SCRAM-SHA-256", principals = { @Principal(user = "alice", password = "foo"),
            @Principal(user = "bob", password = "bar") }) KafkaCluster cluster) {
        doAuthExpectSucceeds(cluster, "alice", "foo");
        doAuthExpectSucceeds(cluster, "bob", "bar");
    }

    private void doAuthExpectSucceeds(KafkaCluster cluster, String username, String password) {
        var config = cluster.getKafkaClientConfiguration(username, password);
        try (var admin = Admin.create(config)) {
            var dcr = admin.describeCluster();
            assertThat(dcr.clusterId())
                    .succeedsWithin(Duration.ofSeconds(10), STRING)
                    .isEqualTo(cluster.getClusterId());
        }
    }

    @Test
    void saslPlainAuthFails(@BrokerCluster @SaslMechanism(principals = { @Principal(user = "alice", password = "foo") }) KafkaCluster cluster) {
        var config = cluster.getKafkaClientConfiguration("alicex", "bad");
        try (var admin = Admin.create(config)) {
            var dcr = admin.describeCluster();
            assertThat(dcr.clusterId())
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableThat()
                    .havingRootCause()
                    .isInstanceOf(SaslAuthenticationException.class)
                    .withMessage("Authentication failed: Invalid username or password");
        }
    }

    @Test
    void tlsClusterParameter(
                             @Tls @BrokerCluster(numBrokers = 1) KafkaCluster cluster,
                             Admin admin)
            throws Exception {
        var bootstrapServer = cluster.getBootstrapServers();
        assertThat(bootstrapServer)
                .withFailMessage("expect a single bootstrap server")
                .doesNotContain(",");

        var listenerPattern = Pattern.compile("(?<listenerName>[a-zA-Z]+)://" + Pattern.quote(bootstrapServer));
        var broker = new ConfigResource(ConfigResource.Type.BROKER, "0");
        var brokerConfigs = admin.describeConfigs(List.of(broker)).all().get().get(broker);
        var advertisedListener = brokerConfigs.get("advertised.listeners").value();
        // e.g. advertisedListener = "EXTERNAL://localhost:37565,INTERNAL://localhost:35173"
        var matcher = listenerPattern.matcher(advertisedListener);
        assertThat(matcher.find())
                .withFailMessage("Expected '" + advertisedListener + "' to contain a match for " + listenerPattern.pattern())
                .isTrue();

        var listenerName = matcher.group("listenerName");
        var protocolMap = brokerConfigs.get("listener.security.protocol.map").value();
        assertThat(protocolMap)
                .withFailMessage("Expected '" + protocolMap + "' to contain " + listenerName + ":SSL")
                .contains(listenerName + ":SSL");
    }

    @Test
    void topic(KafkaCluster cluster,
               Topic topic,
               Admin admin)
            throws Exception {

        ObjectAssert<Topic> topicAssert = assertThat(topic).isNotNull();
        topicAssert.extracting(Topic::name).isNotNull();
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();

        var result = admin.describeTopics(List.of(topic.name())).allTopicNames().get(5, TimeUnit.SECONDS);
        assertThat(result)
                .describedAs("expected topic to exist on the cluster")
                .containsKey(topic.name());
        Uuid topicId = topic.topicId().orElseThrow();
        var result2 = admin.describeTopics(TopicCollection.ofTopicIds(List.of(topicId))).allTopicIds().get(5, TimeUnit.SECONDS);
        assertThat(result2)
                .describedAs("expected topic with id to exist on the cluster")
                .containsKey(topicId);
    }

    @SuppressWarnings("unused") // used via @TopicNameMethodSource
    static String topicName() {
        return FIXED_TOPIC_NAME;
    }

    @Test
    void topicNamingSource(KafkaCluster cluster,
                           @TopicNameMethodSource Topic topic,
                           Admin admin)
            throws Exception {

        ObjectAssert<Topic> topicAssert = assertThat(topic).isNotNull();
        topicAssert.extracting(Topic::name, STRING).isNotNull().isEqualTo(FIXED_TOPIC_NAME);
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();

        var result = admin.describeTopics(List.of(topic.name())).allTopicNames().get(5, TimeUnit.SECONDS);
        assertThat(result)
                .describedAs("expected topic to exist on the cluster")
                .containsKey(topic.name());
        Uuid topicId = topic.topicId().orElseThrow();
        var result2 = admin.describeTopics(TopicCollection.ofTopicIds(List.of(topicId))).allTopicIds().get(5, TimeUnit.SECONDS);
        assertThat(result2)
                .describedAs("expected topic with id to exist on the cluster")
                .containsKey(topicId);
    }

    @Test
    void topicNamingSourceCustomMethodOnAnotherClass(KafkaCluster cluster,
                                                     @TopicNameMethodSource(clazz = AbstractExtensionTest.class, value = "anotherCustomTopicName") Topic topic,
                                                     Admin admin)
            throws Exception {

        ObjectAssert<Topic> topicAssert = assertThat(topic).isNotNull();
        topicAssert.extracting(Topic::name, STRING).isNotNull().isEqualTo(ANOTHER_FIXED_TOPIC_NAME);
        topicAssert.extracting(Topic::topicId, OPTIONAL).isNotNull().isNotEmpty();

        var result = admin.describeTopics(List.of(topic.name())).allTopicNames().get(5, TimeUnit.SECONDS);
        assertThat(result)
                .describedAs("expected topic to exist on the cluster")
                .containsKey(topic.name());
        Uuid topicId = topic.topicId().orElseThrow();
        var result2 = admin.describeTopics(TopicCollection.ofTopicIds(List.of(topicId))).allTopicIds().get(5, TimeUnit.SECONDS);
        assertThat(result2)
                .describedAs("expected topic with id to exist on the cluster")
                .containsKey(topicId);
    }

    @Test
    void topicConfig(KafkaCluster cluster,
                     @TopicConfig(name = CLEANUP_POLICY_CONFIG, value = CLEANUP_POLICY_COMPACT) Topic topic,
                     Admin admin)
            throws Exception {
        var resourceKey = new ConfigResource(ConfigResource.Type.TOPIC, topic.name());
        var all = admin.describeConfigs(List.of(resourceKey)).all().get(5, TimeUnit.SECONDS);

        var topicConfig = all.get(resourceKey);
        assertThat(topicConfig).isNotNull();
        assertConfigValue(topicConfig, CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
    }

    @Test
    void topicWithSpecifiedPartitions(KafkaCluster cluster,
                                      @TopicPartitions(2) Topic topic,
                                      Admin admin)
            throws Exception {
        var result = admin.describeTopics(List.of(topic.name())).allTopicNames().get(5, TimeUnit.SECONDS);
        assertThat(result)
                .describedAs("expected topic to exist on the cluster")
                .containsKey(topic.name());

        var topicDescription = result.get(topic.name());
        assertThat(topicDescription)
                .extracting(TopicDescription::partitions, list(TopicPartitionInfo.class))
                .hasSize(2);
    }

    @Test
    void topicWithSpecifiedReplicationFactor(@BrokerCluster(numBrokers = 2) KafkaCluster cluster,
                                             @TopicReplicationFactor(2) Topic topic,
                                             Admin admin)
            throws Exception {
        var result = admin.describeTopics(List.of(topic.name())).allTopicNames().get(5, TimeUnit.SECONDS);
        assertThat(result)
                .describedAs("expected topic to exist on the cluster")
                .containsKey(topic.name());

        var topicDescription = result.get(topic.name());
        assertThat(topicDescription)
                .extracting(TopicDescription::partitions, list(TopicPartitionInfo.class))
                .singleElement()
                .extracting(TopicPartitionInfo::replicas, list(Node.class))
                .hasSize(2);
    }

    @Test
    void topicGetsBrokerDefaults(@BrokerConfig(name = "num.partitions", value = "2") KafkaCluster cluster,
                                 Topic topic,
                                 Admin admin)
            throws Exception {

        var result = admin.describeTopics(List.of(topic.name())).allTopicNames().get(5, TimeUnit.SECONDS);
        assertThat(result)
                .describedAs("expected topic to exist on the cluster")
                .containsKey(topic.name());

        var topicDescription = result.get(topic.name());
        assertThat(topicDescription)
                .describedAs("expected topic to have default number of partitions")
                .extracting(TopicDescription::partitions)
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .hasSize(2);
    }

    @NonNull
    private String createTopic(Admin admin) throws Exception {
        var topic = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(5, TimeUnit.SECONDS);

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                n -> n.containsKey(new NewTopic(topic, 1, (short) 1).name()));
        return topic;
    }

    private void assertClusterIdAndSize(KafkaCluster cluster, int expectedNodes) throws Exception {
        var dc = describeCluster(cluster.getKafkaClientConfiguration());
        assertThat(dc.clusterId())
                .succeedsWithin(Duration.ofSeconds(10), STRING)
                .isEqualTo(cluster.getClusterId());

        await().atMost(CLUSTER_FORMATION_TIMEOUT).untilAsserted(() -> assertThat(dc.nodes())
                .succeedsWithin(Duration.ofSeconds(10))
                .asInstanceOf(collection(Node.class)).hasSize(expectedNodes));
    }

    private void assertClusterSize(KafkaCluster cluster, int expectedNodes) throws Exception {
        var dc = describeCluster(cluster.getKafkaClientConfiguration());
        await().atMost(CLUSTER_FORMATION_TIMEOUT).untilAsserted(() -> assertThat(dc.nodes())
                .succeedsWithin(Duration.ofSeconds(10))
                .asInstanceOf(collection(Node.class)).hasSize(expectedNodes));
    }

    private void assertConfigValue(Config configs, String configName, String expectedValue) {
        assertThat(configs.get(configName))
                .isNotNull()
                .extracting(ConfigEntry::value)
                .isEqualTo(expectedValue);
    }
}
