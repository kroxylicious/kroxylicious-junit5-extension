/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;

import org.apache.kafka.common.utils.Exit;

import io.kroxylicious.testing.kafka.common.KafkaDriver;
import io.kroxylicious.testing.kafka.common.KafkaEndpoints;
import io.kroxylicious.testing.kafka.common.KafkaNode;
import io.kroxylicious.testing.kafka.common.KafkaNodeConfiguration;
import io.kroxylicious.testing.kafka.common.Listener;
import io.kroxylicious.testing.kafka.common.PortAllocator;
import io.kroxylicious.testing.kafka.common.Zookeeper;
import io.kroxylicious.testing.kafka.common.ZookeeperConfig;

/**
 * Configures and manages an in process (within the JVM) Kafka cluster.
 */
public class InVMKafkaDriver implements KafkaDriver, KafkaEndpoints {
    private static final System.Logger LOGGER = System.getLogger(InVMKafkaDriver.class.getName());
    private final Path tempDirectory;

    private final PortAllocator portsAllocator = new PortAllocator();

    public InVMKafkaDriver() {
        try {
            tempDirectory = Files.createTempDirectory("kafka");
            tempDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        trapKafkaSystemExit();
    }

    @Override
    public synchronized EndpointPair getEndpointPair(Listener listener, int nodeId) {
        if (!portsAllocator.hasRegisteredPort(listener, nodeId)) {
            try (PortAllocator.PortAllocationSession portAllocationSession = portsAllocator.allocationSession()) {
                portAllocationSession.allocate(Set.of(listener), nodeId);
            }
        }
        int port = portsAllocator.getPort(listener, nodeId);
        return EndpointPair.builder().bind(new Endpoint("0.0.0.0", port)).connect(new Endpoint("localhost", port)).build();
    }

    @Override
    public KafkaNode createNode(KafkaNodeConfiguration node) {
        return new InVMKafkaKafkaNode(tempDirectory, node);
    }

    @Override
    public Zookeeper createZookeeper(ZookeeperConfig zookeeperConfig) {
        return new InVMZookeeper(zookeeperConfig.getPort(), tempDirectory);
    }

    @Override
    public void close() {
        if (tempDirectory.toFile().exists()) {
            try (var ps = Files.walk(tempDirectory);
                    var s = ps
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)) {
                s.forEach(File::delete);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void trapKafkaSystemExit() {
        Exit.setExitProcedure(InVMKafkaDriver::exitHandler);
        Exit.setHaltProcedure(InVMKafkaDriver::exitHandler);
    }

    @Override
    public void nodeRemoved(KafkaNode node) {
        portsAllocator.deallocate(node.nodeId());
    }

    private static void exitHandler(int statusCode, String message) {
        final IllegalStateException illegalStateException = new IllegalStateException(message);
        LOGGER.log(System.Logger.Level.WARNING, "Kafka tried to exit with statusCode: {0} and message: {1}. Including stacktrace to determine whats at fault",
                statusCode, message, illegalStateException);
    }
}
