/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;

/**
 * The interface Version.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@KafkaClusterConstraint
public @interface Version {
    String LATEST = "latest";

    /**
     * The value of the version, for instance, 3.6.0.  If value "latest" refers to the latest available kafka
     * version.
     *
     * @return the version
     */
    String value();
}
