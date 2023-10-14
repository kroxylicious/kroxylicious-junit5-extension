/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;
import org.jetbrains.annotations.NotNull;

import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.KafkaServer;
import kafka.server.Server;
import kafka.tools.StorageTool;
import scala.Option;

import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.KafkaNode;
import io.kroxylicious.testing.kafka.common.KafkaNodeConfiguration;
import io.kroxylicious.testing.kafka.common.Utils;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

public class InVMKafkaKafkaNode implements KafkaNode {
    private Server server;
    private boolean started;
    private final Path tempDirectory;
    private final KafkaNodeConfiguration node;
    private static final System.Logger LOGGER = System.getLogger(InVMKafkaKafkaNode.class.getName());
    private static final PrintStream LOGGING_PRINT_STREAM = LoggingPrintStream.loggingPrintStream(LOGGER, System.Logger.Level.DEBUG);
    private static final int STARTUP_TIMEOUT = 30;

    public InVMKafkaKafkaNode(Path tempDirectory, KafkaNodeConfiguration node) {
        this.tempDirectory = tempDirectory;
        this.node = node;
        this.started = false;
    }

    @Override
    public int stop(TerminationStyle terminationStyle) {
        if (server != null) {
            server.shutdown();
            server.awaitShutdown();
            server = null;
        }
        this.started = false;
        return 0;
    }

    @Override
    public void start() {
        if (server == null) {
            this.server = buildKafkaServer();
            tryToStartServerWithRetry(server);
        }
    }

    private void tryToStartServerWithRetry(Server server) {
        Utils.awaitCondition(STARTUP_TIMEOUT, TimeUnit.SECONDS)
                .until(() -> {
                    // Hopefully we can remove this once a fix for https://issues.apache.org/jira/browse/KAFKA-14908 actually lands.
                    try {
                        LOGGER.log(System.Logger.Level.DEBUG, "Attempting to start node: {0} with roles: {1}", node.nodeId(),
                                node.rolesConfigString());
                        server.startup();
                        started = true;
                        return true;
                    }
                    catch (Throwable t) {
                        LOGGER.log(System.Logger.Level.WARNING, "failed to start server due to: " + t.getMessage());
                        server.shutdown();
                        server.awaitShutdown();
                        return false;
                    }
                });
    }

    @Override
    public boolean isStopped() {
        return !started;
    }

    @Override
    public int nodeId() {
        return configuration().nodeId();
    }

    @Override
    public boolean isBroker() {
        return configuration().isBroker();
    }

    @Override
    public KafkaNodeConfiguration configuration() {
        return node;
    }

    @NotNull
    private Server buildKafkaServer() {
        KafkaConfig config = buildBrokerConfig();
        Option<String> threadNamePrefix = Option.apply(null);
        if (node.isKraft()) {
            var clusterId = node.getKafkaKraftClusterId();
            var directories = StorageTool.configToLogDirectories(config);
            var metaProperties = StorageTool.buildMetadataProperties(clusterId, config);
            // note ignoreFormatter=true so tolerate a log directory which is already formatted. this is
            // required to support start/stop.
            StorageTool.formatCommand(LOGGING_PRINT_STREAM, directories, metaProperties, MINIMUM_BOOTSTRAP_VERSION, true);
            return instantiateKraftServer(config, threadNamePrefix);
        }
        else {
            return new KafkaServer(config, Time.SYSTEM, threadNamePrefix, false);
        }
    }

    /**
     * We instantiate the KafkaRaftServer reflectively to support running against versions of kafka
     * older than 3.5.0. This is to enable users to downgrade the broker used for embedded testing rather
     * than forcing them to increase their kafka-clients version to 3.5.0 (broker 3.5.0 requires kafka-clients 3.5.0)
     */
    @NotNull
    private Server instantiateKraftServer(KafkaConfig config, Option<String> threadNamePrefix) {
        Object kraftServer = construct(KafkaRaftServer.class, config, Time.SYSTEM)
                .orElseGet(() -> construct(KafkaRaftServer.class, config, Time.SYSTEM, threadNamePrefix).orElseThrow());
        return (Server) kraftServer;
    }

    public Optional<Object> construct(Class<?> clazz, Object... parameters) {
        Constructor<?>[] declaredConstructors = clazz.getDeclaredConstructors();
        return Arrays.stream(declaredConstructors)
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .filter(constructor -> {
                    if (constructor.getParameterCount() != parameters.length) {
                        return false;
                    }
                    boolean allMatch = true;
                    Class<?>[] parameterTypes = constructor.getParameterTypes();
                    for (int i = 0; i < parameters.length; i++) {
                        allMatch = allMatch && parameterTypes[i].isInstance(parameters[i]);
                    }
                    return allMatch;
                }).findFirst().map(constructor -> {
                    try {
                        return constructor.newInstance(parameters);
                    }
                    catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @NotNull
    private KafkaConfig buildBrokerConfig() {
        Properties properties = new Properties();
        properties.putAll(node.getProperties());
        var logsDir = getBrokerLogDir(node.nodeId());
        properties.setProperty(KafkaConfig.LogDirProp(), logsDir.toAbsolutePath().toString());
        LOGGER.log(System.Logger.Level.DEBUG, "Generated config {0}", properties);
        return new KafkaConfig(properties);
    }

    @NotNull
    private Path getBrokerLogDir(int brokerNum) {
        return this.tempDirectory.resolve(String.format("broker-%d", brokerNum));
    }
}
