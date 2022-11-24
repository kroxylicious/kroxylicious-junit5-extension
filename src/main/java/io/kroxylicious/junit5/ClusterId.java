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

// TODO we're currently using this to associated injectables within tests
// but not all provisioning mechanisms support using your own
// cluster id. To the assicating use case would better be handled
// by a separate annotation that isn't @KafkaClusterConstraint
// (because the association is not a prosioner concern, but an extension one)
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface ClusterId {
    String value();
}
