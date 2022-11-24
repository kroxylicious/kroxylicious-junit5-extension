/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Set;

import io.kroxylicious.cluster.InVMKafkaCluster;
import io.kroxylicious.cluster.KafkaCluster;
import io.kroxylicious.cluster.KafkaClusterConfig;
import io.kroxylicious.junit5.constraint.BrokerCluster;
import io.kroxylicious.junit5.constraint.ClusterId;
import io.kroxylicious.junit5.constraint.KRaftCluster;
import io.kroxylicious.junit5.constraint.SaslPlainAuth;
import io.kroxylicious.junit5.constraint.ZooKeeperCluster;

import static io.kroxylicious.junit5.KafkaClusterExtension.kafkaClusterConfig;

public class InVMProvisioningStrategy implements KafkaClusterProvisioningStrategy {

    private static final Set<Class<? extends Annotation>> SUPPORTED_CONSTRAINTS = Set.of(
            ClusterId.class,
            BrokerCluster.class,
            KRaftCluster.class,
            SaslPlainAuth.class,
            ZooKeeperCluster.class);

    @Override
    public boolean supportsAnnotation(Class<? extends Annotation> constraint) {
        return SUPPORTED_CONSTRAINTS.contains(constraint);
    }

    @Override
    public boolean supportsType(Class<? extends KafkaCluster> declarationType) {
        return declarationType.isAssignableFrom(InVMKafkaCluster.class);
    }

    @Override
    public KafkaCluster create(AnnotatedElement sourceElement, Class<? extends KafkaCluster> declarationType) {
        KafkaClusterConfig config = kafkaClusterConfig(sourceElement);
        return new InVMKafkaCluster(config);
    }

    @Override
    public float estimatedProvisioningTimeMs() {
        return 500;
    }
}
