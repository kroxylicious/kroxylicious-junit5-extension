/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to use
 * provide a cluster that supports SASL-PLAIN configured with the
 * given users.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface SaslPlainAuth {

    /**
     * @return The configured users, which must be non-empty.
     */
    UserPassword[] value();

    @interface UserPassword {
        /**
         * @return A user name.
         */
        String user();

        /**
         * @return A password.
         */
        String password();
    }
}
