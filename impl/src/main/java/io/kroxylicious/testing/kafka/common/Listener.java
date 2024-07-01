/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.*;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;

/**
 * Annotation constraining the external listener of the Kafka cluster.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface Listener {
    /**
     * JAAS server configs.
     *
     * @return jaas server configs
     */
    JaasConfig[] jaasServerConfigs() default {};

    /**
     * JAAS client configs.
     *
     * @return jaas client configs
     */
    JaasConfig[] jaasClientConfigs() default {};

    /**
     * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to
     * provide a cluster with an external listener configured with the given
     * Jaas configuration.
     */
    @Target({ ElementType.PARAMETER, ElementType.FIELD })
    @Retention(RetentionPolicy.RUNTIME)
    @interface JaasConfig {
        /**
         * the config name.
         *
         * @return config name
         */
        String name();

        /**
         * the config value.
         *
         * @return config value
         */
        String value();

        /**
         * The interface List.
         */
        @Retention(RetentionPolicy.RUNTIME)
        @Target({ ElementType.FIELD, ElementType.PARAMETER })
        @interface List {
            /**
             * List of jaas configs.
             *
             * @return the value of the jaas config list
             */
            JaasConfig[] value();
        }

    }
}
