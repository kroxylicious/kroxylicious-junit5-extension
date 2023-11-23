/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.*;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;

/**
 * {@link TopicConfig} is used to provide topic configuration. It may be applied to the {@link Topic} type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Repeatable(TopicConfig.List.class)
@KafkaClusterConstraint
public @interface TopicConfig {
    /**
     * The name of the kafka topic configuration parameter.
     *
     * @return the name
     **/
    String name();

    /**
     * The value of the kafka topic configuration parameter.
     *
     * @return the value
     */
    String value();

    /**
     * The interface List.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @KafkaClusterConstraint
    @interface List {
        /**
         * List of kafka topic configurations.
         *
         * @return the value of the config list
         */
        TopicConfig[] value();
    }
}
