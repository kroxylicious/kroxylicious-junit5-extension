/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.config.SslConfigs;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;

import kafka.server.KafkaConfig;

import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.KafkaNode;
import io.kroxylicious.testing.kafka.common.KafkaNodeConfiguration;

import static io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaClusterDriver.ANON_PORT;
import static io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaClusterDriver.CLIENT_PORT;
import static io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaClusterDriver.KAFKA_CONTAINER_MOUNT_POINT;
import static io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaClusterDriver.getBrokerLogDirectory;

public class TestcontainersKafkaNode implements KafkaNode {

    private static final System.Logger LOGGER = System.getLogger(TestcontainersKafkaNode.class.getName());

    // If Kafka runs for less than 500ms, there's almost certainly a problem. This makes it be treated
    // as a startup failure.
    private static final Duration MINIMUM_RUNNING_DURATION = Duration.ofMillis(500);
    private static final int CONTAINER_STARTUP_ATTEMPTS = 3;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private final TestcontainersKafkaClusterDriver.KafkaContainer kafkaContainer;
    private final KafkaNodeConfiguration nodeConfiguration;

    public TestcontainersKafkaNode(DockerImageName imageName, KafkaNodeConfiguration nodeConfiguration, TestcontainersKafkaClusterDriver.ZookeeperContainer container,
                                   String name,
                                   Network network, String logDirVolumeName) {
        kafkaContainer = buildKafkaContainer(nodeConfiguration, container, name, network, imageName, logDirVolumeName);
        this.nodeConfiguration = nodeConfiguration;
    }

    private static TestcontainersKafkaClusterDriver.KafkaContainer buildKafkaContainer(KafkaNodeConfiguration nodeConfiguration,
                                                                                       TestcontainersKafkaClusterDriver.ZookeeperContainer zookeeper,
                                                                                       String name, Network network, DockerImageName imageName, String logDirVolumeName) {
        String netAlias = "broker-" + nodeConfiguration.nodeIdString();
        Properties properties = new Properties();
        properties.putAll(nodeConfiguration.getProperties());
        properties.put("log.dir", getBrokerLogDirectory(nodeConfiguration.nodeId()));
        TestcontainersKafkaClusterDriver.KafkaContainer kafkaContainer = new TestcontainersKafkaClusterDriver.KafkaContainer(imageName)
                .withName(name)
                .withNetwork(network)
                .withNetworkAliases(netAlias);

        copyHostKeyStoreToContainer(kafkaContainer, properties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        copyHostKeyStoreToContainer(kafkaContainer, properties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);

        kafkaContainer
                .withEnv("SERVER_PROPERTIES_FILE", "/cnf/server.properties")
                .withEnv("SERVER_CLUSTER_ID", nodeConfiguration.getKafkaKraftClusterId())
                // disables automatic configuration of listeners/roles by kafka-native
                .withEnv("SERVER_AUTO_CONFIGURE", "false")
                .withCopyToContainer(Transferable.of(propertiesToBytes(properties), 0644), "/cnf/server.properties")
                .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
                .withMinimumRunningDuration(MINIMUM_RUNNING_DURATION)
                .withStartupTimeout(STARTUP_TIMEOUT);

        if (nodeConfiguration.isBroker()) {
            kafkaContainer.addFixedExposedPort(nodeConfiguration.externalPort(), CLIENT_PORT);
            kafkaContainer.addFixedExposedPort(nodeConfiguration.anonPort(), ANON_PORT);
        }
        kafkaContainer.addGenericBind(new Bind(logDirVolumeName, new Volume(KAFKA_CONTAINER_MOUNT_POINT)));

        if (!nodeConfiguration.isKraft()) {
            kafkaContainer.dependsOn(zookeeper);
        }
        return kafkaContainer;
    }

    private static void copyHostKeyStoreToContainer(TestcontainersKafkaClusterDriver.KafkaContainer container, Properties properties, String key) {
        if (properties.get(key) != null) {
            try {
                var hostPath = Path.of(String.valueOf(properties.get(key)));
                var containerPath = Path.of("/cnf", hostPath.getFileName().toString());
                properties.put(key, containerPath.toString());
                container.withCopyToContainer(Transferable.of(Files.readAllBytes(hostPath), 0644), containerPath.toString());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static byte[] propertiesToBytes(Properties properties) {
        try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
            properties.store(byteArrayOutputStream, "server.properties");
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int stop(TerminationStyle terminationStyle) {
        if (terminationStyle == TerminationStyle.GRACEFUL) {
            gracefulStop(kafkaContainer);
        }
        else {
            kafkaContainer.stop();
        }
        if (nodeConfiguration.isKraft()) {
            return 0;
        }
        else {
            String timeoutMs = (String) nodeConfiguration.getProperties().getOrDefault(KafkaConfig.ZkSessionTimeoutMsProp(), "0");
            return Integer.parseInt(timeoutMs);
        }
    }

    private void gracefulStop(TestcontainersKafkaClusterDriver.KafkaContainer kafkaContainer) {
        // https://github.com/testcontainers/testcontainers-java/issues/1000
        // Note that GenericContainer#stop actually implements stop using kill, so the broker doesn't have chance to
        // tell the controller that it is going away.

        var containerId = kafkaContainer.getContainerId();
        try (var stopContainerCmd = kafkaContainer.getDockerClient().stopContainerCmd(containerId);
                var waitCmd = kafkaContainer.getDockerClient().waitContainerCmd(containerId)) {
            stopContainerCmd.exec();
            var statusCode = waitCmd.start().awaitStatusCode(10, TimeUnit.SECONDS);
            LOGGER.log(System.Logger.Level.DEBUG, "Shut-down broker {0}, exit status {1}", containerId, statusCode);
        }
        catch (Exception e) {
            LOGGER.log(System.Logger.Level.WARNING, "Ignoring exception whilst shutting down broker {0}", containerId, e);
        }
        finally {
            // We need to do this regardless so that Testcontainer's internal state is correct.
            kafkaContainer.stop();
        }
    }

    @Override
    public void start() {
        try {
            Startables.deepStart(kafkaContainer).get(STARTUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isStopped() {
        return !kafkaContainer.isRunning();
    }

    @Override
    public int nodeId() {
        return nodeConfiguration.nodeId();
    }

    @Override
    public boolean isBroker() {
        return nodeConfiguration.isBroker();
    }

    @Override
    public KafkaNodeConfiguration configuration() {
        return nodeConfiguration;
    }
}
