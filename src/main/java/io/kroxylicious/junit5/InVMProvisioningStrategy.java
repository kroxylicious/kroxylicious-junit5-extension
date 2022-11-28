/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;

import io.kroxylicious.cluster.InVMKafkaCluster;
import io.kroxylicious.cluster.KafkaCluster;
import io.kroxylicious.cluster.KafkaClusterConfig;
import io.kroxylicious.junit5.constraint.BrokerCluster;
import io.kroxylicious.junit5.constraint.BrokerConfig;
import io.kroxylicious.junit5.constraint.ClusterId;
import io.kroxylicious.junit5.constraint.KRaftCluster;
import io.kroxylicious.junit5.constraint.SaslPlainAuth;
import io.kroxylicious.junit5.constraint.ZooKeeperCluster;

public class InVMProvisioningStrategy implements KafkaClusterProvisioningStrategy {

    private static final Set<Class<? extends Annotation>> SUPPORTED_CONSTRAINTS = Set.of(
            ClusterId.class,
            BrokerCluster.class,
            BrokerConfig.class,
            BrokerConfig.List.class,
            KRaftCluster.class,
            SaslPlainAuth.class,
            ZooKeeperCluster.class);

    @Override
    public boolean supportsAnnotation(Annotation constraint) {
        return SUPPORTED_CONSTRAINTS.contains(constraint.annotationType());
    }

    @Override
    public boolean supportsType(Class<? extends KafkaCluster> declarationType) {
        return declarationType.isAssignableFrom(InVMKafkaCluster.class);
    }

    @Override
    public KafkaCluster create(List<Annotation> annotationList, Class<? extends KafkaCluster> declarationType) {
        KafkaClusterConfig config = KafkaClusterProvisioningStrategy.kafkaClusterConfig(annotationList);
        return new InVMKafkaCluster(config);
    }

    @Override
    public float estimatedProvisioningTimeMs(List<Annotation> annotationList, Class<? extends KafkaCluster> declarationType) {
        return 500;
    }
}
