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
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to
 * provide a cluster with SASL enabled listener with the given user.
 * <br/>
 * If a @{@link SaslMechanism} constraint is not also provided, SASL PLAIN
 * is assumed.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(User.List.class)
@KafkaClusterConstraint
public @interface User {

    String user();

    String password();

    /**
     * The interface User password.
     */
    @Target({ ElementType.FIELD, ElementType.PARAMETER })
    @Retention(RetentionPolicy.RUNTIME)
    @KafkaClusterConstraint
    @interface List {
        User[] value();
    }
}
