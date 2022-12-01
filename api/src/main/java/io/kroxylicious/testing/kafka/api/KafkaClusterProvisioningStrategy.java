/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.api;

import java.lang.annotation.Annotation;
import java.util.List;

/**
 * Service interface for provisioning.
 */
public interface KafkaClusterProvisioningStrategy {

    /**
     * @param constraint A {@link KafkaClusterConstraint}-annotated constraint annotation.
     * @return Whether this provisioning strategy supports/understands the given {@code constraint}.
     */
    boolean supportsAnnotation(Annotation constraint);

    /**
     * @param declarationType The specific subtype of {@link KafkaCluster}
     * @return Whether this provisioning strategy supports creating instances that
     * are a subclass of the given {@code declarationType}.
     */
    boolean supportsType(Class<? extends KafkaCluster> declarationType);

    /**
     * Estminate the time it would take to provision a cluster with the given configuration.
     * @param constraints The {@link KafkaClusterConstraint}-annotated constraint annotations
     * @param declarationType The specific subtype of {@link KafkaCluster} to be created.
     * @return The estimated provisioning time (including the time taken for {@link KafkaCluster#start()}.
     */
    float estimatedProvisioningTimeMs(List<Annotation> constraints,
                                      Class<? extends KafkaCluster> declarationType);

    /**
     * Create a {@link KafkaCluster} instance with the given configuration.
     * @param constraints The constraints.
     * @param declarationType The subtype.
     * @return The created instance.
     */
    KafkaCluster create(List<Annotation> constraints,
                        Class<? extends KafkaCluster> declarationType);

}
