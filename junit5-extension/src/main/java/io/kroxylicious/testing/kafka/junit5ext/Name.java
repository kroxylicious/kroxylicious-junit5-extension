/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaCluster;

/**
 * <p>Disambiguating annotation for declarations of type {@link KafkaCluster},
 * {@link org.apache.kafka.clients.producer.Producer} etc.
 * when there are multiple {@code KafkaClusters} in scope.</p>
 *
 * <p><For example, consider the following declaration:
 *
 * <pre>{@code
 * KafkaCluster clusterA;
 * KafkaCluster clusterB;
 * @Test
 * public void myTest(Producer<String, String> producer)
 * }</pre>
 *
 * <p>{@link KafkaClusterExtension} will fail parameter resolution with
 * {@link AmbiguousKafkaClusterException} in this case
 * because it's not clear whether {@code producer} should be configured for
 * {@code clusterA} or {@code clusterB}.</p>
 *
 * <p>You can disambiguate the code using {@code @Name}, like this:</p>
 * <pre>{@code
 * @Name("A") KafkaCluster clusterA;
 * @Name("B") KafkaCluster clusterB;
 * @Test
 * public void myTest(@Name("A") Producer<String, String> producer)
 * }</pre>
 * <p>Where the {@code @Name} on the declaration of {@code producer}
 * associates it with {@code clusterA}.</p>
 *
 *
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Name {
    String value();
}
