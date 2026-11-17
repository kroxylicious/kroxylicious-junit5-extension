/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka;

import org.apache.kafka.common.utils.internals.AppInfoParser;

public final class KafkaVersionTestUtil {

    private KafkaVersionTestUtil() {
    }

    public static String getAppInfoParserVersion() {
        try {
            return AppInfoParser.getVersion();
        }
        catch (LinkageError e) {
            // AppInfoParser moved from org.apache.kafka.common.utils to org.apache.kafka.common.utils.internals in Kafka 4.4.
            try {
                var clazz = Class.forName("org.apache.kafka.common.utils.AppInfoParser");
                return (String) clazz.getMethod("getVersion").invoke(null);
            }
            catch (ReflectiveOperationException ex) {
                throw new IllegalStateException("failed to determine kafka version from classpath", ex);
            }
        }
    }
}
