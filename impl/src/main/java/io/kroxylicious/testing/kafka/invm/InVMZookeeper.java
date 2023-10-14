/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import io.kroxylicious.testing.kafka.common.Zookeeper;

public class InVMZookeeper implements Zookeeper {

    private final int port;
    private final ZooKeeperServer zooServer;

    public InVMZookeeper(int port, Path tempDir) {
        this.port = port;
        var zoo = tempDir.resolve("zoo");
        var snapshotDir = zoo.resolve("snapshot");
        var logDir = zoo.resolve("log");
        try {
            Files.createDirectories(snapshotDir);
            Files.createDirectories(logDir);
            zooServer = new ZooKeeperServer(snapshotDir.toFile(), logDir.toFile(), 500);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        zooServer.shutdown();
    }

    @Override
    public void start() {
        try {
            ServerCnxnFactory zooFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
            zooFactory.startup(zooServer);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
