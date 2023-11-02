/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;

/**
 * {@code @KafkaConfig} can be used to annotate a field in a test class or a
 *  parameter in a lifecycle method or test method in order to provide Kafka configuration to it.
 *  <br/>
 *  The following types are supported:
 *  <ol>
 *      <li>{@link io.kroxylicious.testing.kafka.api.KafkaCluster}</li>
 *      <li>{@link org.apache.kafka.clients.admin.AdminClient}</li>
 *      <li>{@link org.apache.kafka.clients.producer.Producer}</li>
 *      <li>{@link org.apache.kafka.clients.consumer.Consumer}</li>
 *  </ol>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Repeatable(KafkaConfig.List.class)
@KafkaClusterConstraint
public @interface KafkaConfig {
    /**
     * The name of the kafka configuration parameter.
     *
     * @return the name
     **/
    String name();

    /**
     * The value of the kafka configuration parameter.
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
         * List of kafka configurations.
         *
         * @return the value of the config list
         */
        KafkaConfig[] value();
    }
}
