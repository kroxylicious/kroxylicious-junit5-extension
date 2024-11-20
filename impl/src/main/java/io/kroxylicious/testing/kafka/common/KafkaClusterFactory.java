/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

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
     * @deprecated use the field from {@link TestcontainersKafkaCluster}
     */
    @Deprecated(forRemoval = true)
    public static final String KAFKA_IMAGE_REPO = TestcontainersKafkaCluster.KAFKA_IMAGE_REPO;

    @Deprecated(forRemoval = true)
    public static final String ZOOKEEPER_IMAGE_REPO = TestcontainersKafkaCluster.ZOOKEEPER_IMAGE_REPO;

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
        var kafkaVersion = getKafkaVersion(clusterConfig);
        var kraftMode = convertClusterKraftMode(System.getenv().get(TEST_CLUSTER_KRAFT_MODE), true);
        var builder = clusterConfig.toBuilder();

        if (clusterConfig.getExecMode() == null) {
            builder.execMode(clusterMode);
        }

        if (clusterConfig.getKraftMode() == null) {
            builder.kraftMode(kraftMode);
        }

        builder.kafkaVersion(kafkaVersion);

        var actual = builder.build();
        LOGGER.log(DEBUG, "Test cluster : {0}", actual);

        if (actual.getExecMode() == KafkaClusterExecutionMode.IN_VM) {
            return new InVMKafkaCluster(actual);
        }
        else {
            return new TestcontainersKafkaCluster(actual);
        }
    }

    private static KafkaClusterExecutionMode getExecutionMode(KafkaClusterConfig clusterConfig) {
        return KafkaClusterExecutionMode.convertClusterExecutionMode(System.getenv().get(TEST_CLUSTER_EXECUTION_MODE),
                clusterConfig.getExecMode() == null ? KafkaClusterExecutionMode.IN_VM : clusterConfig.getExecMode());
    }

    private static String getKafkaVersion(KafkaClusterConfig clusterConfig) {
        return System.getenv().getOrDefault(KAFKA_VERSION, clusterConfig.getKafkaVersion());
    }

    private static boolean convertClusterKraftMode(String mode, boolean defaultMode) {
        if (mode == null) {
            return defaultMode;
        }
        return Boolean.parseBoolean(mode);
    }
}
