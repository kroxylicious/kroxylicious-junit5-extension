/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;
import org.testcontainers.utility.DockerImageName;

import static java.lang.System.Logger.Level.INFO;

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

    public static KafkaCluster create(KafkaClusterConfig clusterConfig) {
        if (clusterConfig == null) {
            throw new NullPointerException();
        }

        var clusterMode = KafkaClusterExecutionMode.convertClusterExecutionMode(System.getenv().get(TEST_CLUSTER_EXECUTION_MODE), KafkaClusterExecutionMode.IN_VM);
        var kraftMode = convertClusterKraftMode(System.getenv().get(TEST_CLUSTER_KRAFT_MODE), true);

        var builder = clusterConfig.toBuilder();

        if (clusterConfig.getExecMode() == null) {
            builder.execMode(clusterMode);
        }

        if (clusterConfig.getKraftMode() == null) {
            builder.kraftMode(kraftMode);
        }

        var kafkaVersion = System.getenv().getOrDefault(KAFKA_VERSION, "latest");
        builder.kafkaVersion(kafkaVersion);

        var actual = builder.build();
        LOGGER.log(INFO, "Test cluster : {0}", actual);

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

    private static boolean convertClusterKraftMode(String mode, boolean defaultMode) {
        if (mode == null) {
            return defaultMode;
        }
        return Boolean.parseBoolean(mode);
    }

}
