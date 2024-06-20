/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import kafka.server.KafkaConfig;
import kafka.tools.StorageTool;
import scala.collection.immutable.Seq;

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

    static void prepareLogDirsForKraft(String clusterId, KafkaConfig config, List<UserScramCredentialRecord> scramCredentialRecords) {
        var directories = StorageTool.configToLogDirectories(config);
        try {
            // Default the metadata version from the IBP version specified in config in the same way as kafka.tools.StorageTool.
            var metadataVersion = MetadataVersion.fromVersionString(
                    config.interBrokerProtocolVersionString() == null ? MetadataVersion.LATEST_PRODUCTION.version() : config.interBrokerProtocolVersionString());

            // in kafka 3.7.0 the MetadataProperties class moved package, we use reflection to enable the extension to work with
            // the old and new class.
            var metaProperties = buildMetadataPropertiesReflectively(clusterId, config);
            var bootstrapMetadata = buildBootstrapMetadata(scramCredentialRecords, metadataVersion);

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

    private static BootstrapMetadata buildBootstrapMetadata(List<UserScramCredentialRecord> scramCredentialRecords, MetadataVersion metadataVersion) {
        var metadataRecords = new ArrayList<ApiMessageAndVersion>();
        metadataRecords.add(metadataVersionMessage(metadataVersion));
        for (var credentialRecord : scramCredentialRecords) {
            metadataRecords.add(scramMessage(credentialRecord));
        }
        return BootstrapMetadata.fromRecords(metadataRecords, InVMKafkaCluster.INVM_KAFKA);
    }

    private static Object buildMetadataPropertiesReflectively(String clusterId, KafkaConfig config)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        var buildMetadataProperties = StorageTool.class.getDeclaredMethod("buildMetadataProperties", String.class, KafkaConfig.class);
        return buildMetadataProperties.invoke(null, clusterId, config);
    }

    private static ApiMessageAndVersion withVersion(ApiMessage message) {
        return new ApiMessageAndVersion(message, (short) 0);
    }

    private static ApiMessageAndVersion metadataVersionMessage(MetadataVersion metadataVersion) {
        return withVersion(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(metadataVersion.featureLevel()));
    }

    private static ApiMessageAndVersion scramMessage(UserScramCredentialRecord scramRecord) {
        return withVersion(scramRecord);
    }
}
