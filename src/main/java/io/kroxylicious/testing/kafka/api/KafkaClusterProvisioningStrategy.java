/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.api;

import java.lang.annotation.Annotation;
import java.util.List;

public interface KafkaClusterProvisioningStrategy {

    // This implies that the extension knows how to create a config from the annotations
    // which implies hard-coded annotations
    // We actually need to know:
    // a. Which annotations are constraints (meta-annotation)
    // b. Find all provisioning strategies which support all the annotations on the decl
    // c. Filter for decl type
    // d. Move the creation of config from annotations into the strategy
    boolean supportsAnnotation(Annotation constraint);
    // TODO this ^^ doesn't cope with the possibility that it's the combination of
    // constraints that's the problem
    // But having a per-constraint method is helpful for debugging
    // why a provisioner got ruled out
    // To fix that create() should be allowed to throw or otherwise express the inability
    // to actually consume the whole config.

    boolean supportsType(Class<? extends KafkaCluster> declarationType);

    KafkaCluster create(List<Annotation> sourceElement,
                        Class<? extends KafkaCluster> declarationType);

    // TODO logically the time depends on the configuration
    float estimatedProvisioningTimeMs(List<Annotation> sourceElement,
                                      Class<? extends KafkaCluster> declarationType);

}
