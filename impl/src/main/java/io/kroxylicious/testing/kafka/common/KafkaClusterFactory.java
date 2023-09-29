/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static java.lang.System.Logger.Level.DEBUG;

/**
 * The type Kafka cluster factory.
 */
public class KafkaClusterFactory {
    private static final System.Logger LOGGER = System.getLogger(KafkaClusterFactory.class.getName());

    /**
     * environment variable specifying execution mode, IN_VM or CONTAINER.
     */
    public static final String TEST_CLUSTER_EXECUTION_MODE = "TEST_CLUSTER_EXECUTION_MODE";

    /**
     * environment variable specifying the kafka version.
     */
    public static final String KAFKA_VERSION = "KAFKA_VERSION";

    /**
     * environment variable specifying the kafka image repository.
     */
    public static final String KAFKA_IMAGE_REPO = "KAFKA_IMAGE_REPO";

    /**
     * environment variable specifying the zookeeper image repository.
     */
    public static final String ZOOKEEPER_IMAGE_REPO = "ZOOKEEPER_IMAGE_REPO";

    /**
     * environment variable specifying kraft mode, true or false.
     */
    public static final String TEST_CLUSTER_KRAFT_MODE = "TEST_CLUSTER_KRAFT_MODE";

    /**
     * Instantiates a new Kafka cluster factory.
     */
    public KafkaClusterFactory() {
    }

    /**
     * Create kafka cluster.
     *
     * @param clusterConfig the cluster config
     * @return the kafka cluster
     */
    public static KafkaCluster create(KafkaClusterConfig clusterConfig) {
        if (clusterConfig == null) {
            throw new NullPointerException();
        }

        var clusterMode = getExecutionMode(clusterConfig);
        var kraftMode = convertClusterKraftMode(System.getenv().get(TEST_CLUSTER_KRAFT_MODE), MetadataMode.KRAFT_COMBINED);
        var builder = clusterConfig.toBuilder();

        if (clusterConfig.getExecMode() == null) {
            builder.execMode(clusterMode);
        }

        if (clusterConfig.getMetadataMode() == null) {
            builder.metadataMode(kraftMode);
        }

        if (KafkaClusterExecutionMode.CONTAINER == clusterMode
                && kraftMode != MetadataMode.ZOOKEEPER
                && clusterConfig.getBrokersNum() < clusterConfig.getKraftControllers()) {
            throw new IllegalStateException(
                    "Due to https://github.com/ozangunalp/kafka-native/issues/88 we can't support controller only nodes in " + KafkaClusterExecutionMode.CONTAINER
                            + " mode so we need to fail fast. This cluster has "
                            + clusterConfig.getBrokersNum() + " brokers and " + clusterConfig.getKraftControllers() + " controllers");
        }

        var kafkaVersion = System.getenv().getOrDefault(KAFKA_VERSION, "latest");
        builder.kafkaVersion(kafkaVersion);

        var actual = builder.build();
        LOGGER.log(DEBUG, "Test cluster : {0}", actual);

        if (actual.getExecMode() == KafkaClusterExecutionMode.IN_VM) {
            return new InVMKafkaCluster(actual);
        }
        else {
            var kafkaImageRepo = System.getenv().get(KAFKA_IMAGE_REPO);
            var zookeeperImageRepo = System.getenv().get(ZOOKEEPER_IMAGE_REPO);
            DockerImageName kafkaImage = (kafkaImageRepo != null) ? DockerImageName.parse(kafkaImageRepo) : null;
            DockerImageName zookeeperImage = (zookeeperImageRepo != null) ? DockerImageName.parse(zookeeperImageRepo) : null;

            return new TestcontainersKafkaCluster(kafkaImage, zookeeperImage, actual);
        }
    }

    private static KafkaClusterExecutionMode getExecutionMode(KafkaClusterConfig clusterConfig) {
        return KafkaClusterExecutionMode.convertClusterExecutionMode(System.getenv().get(TEST_CLUSTER_EXECUTION_MODE),
                clusterConfig.getExecMode() == null ? KafkaClusterExecutionMode.IN_VM : clusterConfig.getExecMode());
    }

    private static MetadataMode convertClusterKraftMode(String mode, MetadataMode defaultMode) {
        if (mode == null) {
            return defaultMode;
        }
        return Boolean.parseBoolean(mode) ? MetadataMode.KRAFT_COMBINED : MetadataMode.ZOOKEEPER;
    }
}
