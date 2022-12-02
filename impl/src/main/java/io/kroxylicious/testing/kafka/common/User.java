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

@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(User.List.class)
@KafkaClusterConstraint
public @interface User {
    /**
     * @return A user name.
     */
    String user();

    /**
     * @return A password.
     */
    String password();

    @Target({ ElementType.PARAMETER, ElementType.FIELD })
    @Retention(RetentionPolicy.RUNTIME)
    @KafkaClusterConstraint
    @interface List {
        User[] value();
    }
}
