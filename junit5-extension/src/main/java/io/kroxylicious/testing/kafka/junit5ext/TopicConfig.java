/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link TopicConfig} is used to provide topic configuration. It may be applied to the {@link Topic} type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Repeatable(TopicConfig.List.class)
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
    @interface List {
        /**
         * List of kafka topic configurations.
         *
         * @return the value of the config list
         */
        TopicConfig[] value();
    }
}
