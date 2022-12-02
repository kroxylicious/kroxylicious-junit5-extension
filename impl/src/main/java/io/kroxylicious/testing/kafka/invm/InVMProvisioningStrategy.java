/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.List;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;

public class InVMProvisioningStrategy implements KafkaClusterProvisioningStrategy {

    @Override
    public boolean supportsAnnotation(Annotation constraint) {
        return KafkaClusterConfig.supportsConstraint(constraint.annotationType());
    }

    @Override
    public boolean supportsType(Class<? extends KafkaCluster> declarationType) {
        return declarationType.isAssignableFrom(InVMKafkaCluster.class);
    }

    @Override
    public KafkaCluster create(List<Annotation> constraints, Class<? extends KafkaCluster> declarationType) {
        KafkaClusterConfig config = KafkaClusterConfig.fromConstraints(constraints);
        return new InVMKafkaCluster(config);
    }

    @Override
    public Duration estimatedProvisioningTimeMs(List<Annotation> constraints, Class<? extends KafkaCluster> declarationType) {
        return Duration.ofMillis(500);
    }
}
