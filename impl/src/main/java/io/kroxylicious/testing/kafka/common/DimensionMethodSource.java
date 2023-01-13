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

/**
 * Reference to a method providing a Stream, Collection or array of constraint annotations
 * for a {@code TestTemplate}.
 * The method will be invoked by the {@link KafkaClusterExtension}
 * to provide a list of constraints over which the test will parameterized.
 * The annotation should be used on a {@code KafkaCluster}-typed parameter of a
 * {@code TestTemplate}-annotated method.
 * The referenced method much be {@code static} and package- or {@code public}ly-accessible.
 *
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class TemplateTest {
 *     @TestTemplate
 *     public void multipleClusterSizes(
 *             @DimensionMethodSource("clusterSizes")
 *             KafkaCluster cluster) throws Exception {
 *         // ... your test code
 *     }
 *
 *     static Stream<BrokerCluster> clusterSizes() {
 *         return Stream.of(
 *             mkAnnotation(BrokerCluster.class, 1),
 *             mkAnnotation(BrokerCluster.class, 3));
 *     }
 * }
 * }</pre>
 *
 * <p>If multiple such annotation are present there will be a test for each element of the Cartesian product
 * over each of the dimensions. If you want to execute a subset of the Cartesian
 * product you might find {@link ConstraintsMethodSource @ConstraintsMethodSource} more convenient.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Repeatable(DimensionMethodSource.List.class)
public @interface DimensionMethodSource {

    /**
     * The name of the {@code static}, package- or {@code public}-accessible method.
     */
    String value();

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.PARAMETER, ElementType.FIELD })
    @interface List {
        DimensionMethodSource[] value();
    }
}
