/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.*;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;

/**
 * {@link TopicConfig} is used to provide client configuration to any of the three Kafka
 * Clients ({@link org.apache.kafka.clients.admin.AdminClient},
 * {@link org.apache.kafka.clients.producer.Producer} and {@link org.apache.kafka.clients.consumer.Consumer}).
 * <br/>
 * The annotation is supported on fields in a test class, or on a parameter in a lifecycle method or test method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Repeatable(TopicConfig.List.class)
@KafkaClusterConstraint
public @interface TopicConfig {
    /**
     * The name of the kafka client configuration parameter.
     *
     * @return the name
     **/
    String name();

    /**
     * The value of the kafka client configuration parameter.
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
         * List of kafka client configurations.
         *
         * @return the value of the config list
         */
        TopicConfig[] value();
    }
}
