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
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;

/**
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to
 * provide a cluster with an external listener configured to expect the given
 * SASL mechanism.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface SaslMechanism {
    /**
     * SASL mechanism name.
     *
     * @return sasl mechanism name
     */
    String value() default "PLAIN";

    /**
     * Principals.
     * <br/>
     * Note: principals are only supported by PLAIN and SCRAM-SHA mechanisms.
     *
     * @return principals
     */
    Principal[] principals() default {};

    /**
     * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to
     * provide a cluster with SASL enabled listener with the given user.
     * <br/>
     * If a @{@link SaslMechanism} constraint is not also provided, SASL PLAIN
     * is assumed.
     */
    @Target({ ElementType.PARAMETER, ElementType.FIELD })
    @Retention(RetentionPolicy.RUNTIME)
    @Repeatable(Principal.List.class)
    @interface Principal {

        String user();

        String password();

        /**
         * The interface User password.
         */
        @Target({ ElementType.FIELD, ElementType.PARAMETER })
        @Retention(RetentionPolicy.RUNTIME)
        @KafkaClusterConstraint
        @interface List {
            Principal[] value();
        }
    }
}
