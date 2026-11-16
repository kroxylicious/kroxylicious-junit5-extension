/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Determines the version of {@code kafka-clients} present on the classpath.
 */
public final class KafkaClientsVersion {

    private static final String VERSION_RESOURCE = "/kafka/kafka-version.properties";

    /** Value returned when the version cannot be determined, matching {@code AppInfoParser}'s default. */
    public static final String UNKNOWN_VERSION = "unknown";

    private KafkaClientsVersion() {
    }

    /**
     * Detect the kafka-clients version from kafka-version.properties on the classpath.
     * <p>
     * Reads the resource directly rather than calling {@code org.apache.kafka.common.utils.AppInfoParser},
     * which Kafka 4.4 relocated to an {@code internals} sub-package, breaking callers compiled against
     * either location when run against the other version.
     *
     * @return the version, or {@link #UNKNOWN_VERSION} if it could not be determined
     */
    public static String detectFromClasspath() {
        var properties = new Properties();
        try (InputStream resourceStream = KafkaClientsVersion.class.getResourceAsStream(VERSION_RESOURCE)) {
            if (resourceStream != null) {
                properties.load(resourceStream);
            }
        }
        catch (IOException e) {
            // ignored, matches AppInfoParser's behaviour of swallowing load failures.
        }
        return properties.getProperty("version", UNKNOWN_VERSION);
    }
}
