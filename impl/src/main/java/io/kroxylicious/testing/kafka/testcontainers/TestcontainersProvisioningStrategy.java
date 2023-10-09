/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.TestInfo;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;

/**
 * The type Testcontainers provisioning strategy.
 */
public class TestcontainersProvisioningStrategy implements KafkaClusterProvisioningStrategy {

    /**
     * Instantiates a new Testcontainers provisioning strategy.
     */
    public TestcontainersProvisioningStrategy() {
    }

    @Override
    public boolean supportsAnnotation(Annotation constraint) {
        return KafkaClusterConfig.supportsConstraint(constraint.annotationType());
    }

    @Override
    public boolean supportsType(Class<? extends KafkaCluster> declarationType) {
        return declarationType.isAssignableFrom(TestcontainersKafkaCluster.class);
    }

    @Override
    public KafkaCluster create(List<Annotation> constraints, Class<? extends KafkaCluster> declarationType, TestInfo testInfo) {
        KafkaClusterConfig config = KafkaClusterConfig.fromConstraints(constraints, testInfo);
        return new TestcontainersKafkaCluster(null, null, config);
    }

    @Override
    public Duration estimatedProvisioningTimeMs(List<Annotation> constraints, Class<? extends KafkaCluster> declarationType) {
        return Duration.ofSeconds(1);
    }
}
