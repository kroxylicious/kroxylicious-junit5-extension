/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import kafka.server.KafkaConfig;
import kafka.tools.StorageTool;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Note that this code is base on code from Kafka's StorageTool.
 */
final class KraftLogDirUtil {
    private static final PrintStream LOGGING_PRINT_STREAM = LoggingPrintStream.loggingPrintStream(InVMKafkaCluster.LOGGER, System.Logger.Level.DEBUG);
    private static final String FORMAT_METHOD_NAME = "formatCommand";
    private static final boolean IGNORE_FORMATTED = true;

    private KraftLogDirUtil() {
        throw new IllegalStateException();
    }

    // we allow it to throw the generic exception type as a way to handle arbitrary failures
    @SuppressWarnings("java:S112")
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Thrown when all fallback alternatives have failed
     */
    private static class FallbackException extends RuntimeException {
        private FallbackException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The intent is to execute the runnables in sequence, returning immediately if one succeeds.
     * @param runnables runnables
     * @throws RuntimeException if all runnables throw an exception
     */
    private static void tryWithFallbacks(ThrowingRunnable... runnables) {
        Throwable lastFailure = null;
        for (ThrowingRunnable runnable : runnables) {
            try {
                runnable.run();
                return;
            }
            catch (LinkageError | Exception e) {
                lastFailure = e;
            }
        }
        throw new FallbackException("all fallbacks failed", lastFailure);
    }

    static void prepareLogDirsForKraft(String clusterId, KafkaConfig config) {
        var metadataVersion = getMetadataVersion(config);
        var directoriesScala = StorageTool.configToLogDirectories(config);
        tryWithFallbacks(() -> prepareLogDirsForKraftKafka41Plus(clusterId, config, directoriesScala, metadataVersion),
                () -> prepareLogDirsForKraftKafka39Plus(clusterId, config, directoriesScala, metadataVersion),
                () -> prepareLogDirsForKraftPreKafka39(clusterId, config, directoriesScala, metadataVersion));
    }

    private static MetadataVersion getMetadataVersion(KafkaConfig config) {
        try {
            String version = ReflectionUtils.invokeInstanceMethod(config, "interBrokerProtocolVersionString");
            return Optional.ofNullable(version).map(MetadataVersion::fromVersionString).orElse(MetadataVersion.LATEST_PRODUCTION);
        }
        catch (Exception e) {
            return MetadataVersion.LATEST_PRODUCTION;
        }
    }

    private static void prepareLogDirsForKraftKafka41Plus(String clusterId, KafkaConfig config, Seq<String> directoriesScala,
                                                          MetadataVersion metadataVersion)
            throws Exception {
        var controllerListenerName = config.controllerListenerNames().stream().findFirst().orElseThrow();
        var directories = CollectionConverters.asJava(directoriesScala);
        Formatter formatter = new Formatter();
        formatter.setClusterId(clusterId);
        formatter.setNodeId(config.nodeId());
        formatter.setControllerListenerName(controllerListenerName);
        formatter.setMetadataLogDirectory(config.metadataLogDir());
        formatter.setDirectories(directories);
        formatter.setIgnoreFormatted(IGNORE_FORMATTED);
        formatter.setPrintStream(LOGGING_PRINT_STREAM);
        formatter.setReleaseVersion(metadataVersion);
        formatter.run();
    }

    private static void prepareLogDirsForKraftKafka39Plus(String clusterId, KafkaConfig config, Seq<String> directoriesScala,
                                                          MetadataVersion metadataVersion)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        // Try Formatter class (introduced Kafka 3.9)
        scala.collection.Seq<String> controllerListenerNameSeq = (scala.collection.Seq<String>) KafkaConfig.class.getMethod("controllerListenerNames")
                .invoke(config);
        var controllerListenerName = CollectionConverters.asJava(controllerListenerNameSeq).stream().findFirst().orElseThrow();
        var directories = CollectionConverters.asJava(directoriesScala);
        var formatterClazz = Class.forName("org.apache.kafka.metadata.storage.Formatter");
        var formatter = formatterClazz.getDeclaredConstructor().newInstance();

        formatterClazz.getMethod("setClusterId", String.class).invoke(formatter, clusterId);
        formatterClazz.getMethod("setNodeId", int.class).invoke(formatter, config.nodeId());
        formatterClazz.getMethod("setControllerListenerName", String.class).invoke(formatter, controllerListenerName);
        formatterClazz.getMethod("setMetadataLogDirectory", String.class).invoke(formatter, config.metadataLogDir());
        formatterClazz.getMethod("setDirectories", Collection.class).invoke(formatter, directories);
        formatterClazz.getMethod("setIgnoreFormatted", boolean.class).invoke(formatter, IGNORE_FORMATTED);
        formatterClazz.getMethod("setPrintStream", PrintStream.class).invoke(formatter, LOGGING_PRINT_STREAM);
        formatterClazz.getMethod("setReleaseVersion", MetadataVersion.class).invoke(formatter, metadataVersion);

        formatterClazz.getMethod("run").invoke(formatter);
    }

    private static void prepareLogDirsForKraftPreKafka39(String clusterId, KafkaConfig config, Seq<String> directories, MetadataVersion metadataVersion) {
        try {
            // Default the metadata version from the IBP version specified in config in the same way as kafka.tools.StorageTool.

            // in kafka 3.7.0 the MetadataProperties class moved package, we use reflection to enable the extension to work with
            // the old and new class.
            var metaProperties = buildMetadataPropertiesReflectively(clusterId, config);
            var bootstrapMetadata = buildBootstrapMetadata(metadataVersion);

            formatReflectively(metaProperties, directories, bootstrapMetadata, metadataVersion);
        }
        catch (Exception e) {
            throw new RuntimeException("failed to prepare log dirs for KRaft", e);
        }
    }

    private static void formatReflectively(Object metaProperties, Seq<String> directories, BootstrapMetadata bootstrapMetadata, MetadataVersion metadataVersion)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        try {
            var formatMethod = StorageTool.class.getDeclaredMethod(FORMAT_METHOD_NAME, PrintStream.class, Seq.class, metaProperties.getClass(), BootstrapMetadata.class,
                    MetadataVersion.class,
                    boolean.class);
            // note ignoreFormatter=true so tolerate a log directory which is already formatted. this is
            // required to support start/stop.
            formatMethod.invoke(null, LOGGING_PRINT_STREAM, directories, metaProperties, bootstrapMetadata, metadataVersion, IGNORE_FORMATTED);
        }
        catch (NoSuchMethodException e) {
            // fallback for pre-kafka 3.6 formatCommand which didn't accept BootstrapMetadata
            var formatMethod = StorageTool.class.getDeclaredMethod(FORMAT_METHOD_NAME, PrintStream.class, Seq.class, metaProperties.getClass(),
                    MetadataVersion.class,
                    boolean.class);
            formatMethod.invoke(null, LOGGING_PRINT_STREAM, directories, metaProperties, metadataVersion, IGNORE_FORMATTED);
        }
    }

    private static BootstrapMetadata buildBootstrapMetadata(MetadataVersion metadataVersion) {
        var metadataRecords = new ArrayList<ApiMessageAndVersion>();
        metadataRecords.add(metadataVersionMessage(metadataVersion));
        return BootstrapMetadata.fromRecords(metadataRecords, KraftLogDirUtil.class.getName());
    }

    private static Object buildMetadataPropertiesReflectively(String clusterId, KafkaConfig config)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        var buildMetadataProperties = StorageTool.class.getDeclaredMethod("buildMetadataProperties", String.class, KafkaConfig.class);
        return buildMetadataProperties.invoke(null, clusterId, config);
    }

    private static ApiMessageAndVersion metadataVersionMessage(MetadataVersion metadataVersion) {
        return wrap(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(metadataVersion.featureLevel()));
    }

    private static ApiMessageAndVersion wrap(ApiMessage message) {
        return new ApiMessageAndVersion(message, (short) 0);
    }
}
