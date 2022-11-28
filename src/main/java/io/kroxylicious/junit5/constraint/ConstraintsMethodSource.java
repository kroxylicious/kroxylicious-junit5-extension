/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5.constraint;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Reference to a method providing a Stream, Collection or array of constraint annotations tuples
 * for a {@code TestTemplate}.
 * The method will be invoked by the {@link io.kroxylicious.junit5.KafkaClusterExtension}
 * to provide a list of constraint lists over which the test will parameterized.
 * The annotation should be used on a {@code KafkaCluster}-typed parameter of a
 * {@code TestTemplate}-annotated method.
 * The referenced method much be {@code static} and package- or {@code public}ly-accessible.
 *
 * <p>If you want to execute a tests for each of the Cartesian product
 * of a number of dimensions you might find {@link DimensionMethodSource @DimensionMethodSource}
 * more convenient.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER, ElementType.FIELD })
public @interface ConstraintsMethodSource {
    String value();
}
