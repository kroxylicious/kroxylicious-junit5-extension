/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.kroxylicious.cluster.KafkaCluster;
import io.kroxylicious.cluster.KafkaClusterConfig;
import io.kroxylicious.junit5.constraint.BrokerCluster;
import io.kroxylicious.junit5.constraint.BrokerConfig;
import io.kroxylicious.junit5.constraint.ClusterId;
import io.kroxylicious.junit5.constraint.KRaftCluster;
import io.kroxylicious.junit5.constraint.SaslPlainAuth;
import io.kroxylicious.junit5.constraint.ZooKeeperCluster;

public interface KafkaClusterProvisioningStrategy {

    public static KafkaClusterConfig kafkaClusterConfig(List<Annotation> annotations) {
        System.Logger logger = System.getLogger(KafkaClusterProvisioningStrategy.class.getName());
        var builder = KafkaClusterConfig.builder();
        builder.brokersNum(1);
        for (Annotation annotation : annotations) {
            if (annotation instanceof BrokerCluster) {
                builder.brokersNum(((BrokerCluster) annotation).numBrokers());
            }
            if (annotation instanceof KRaftCluster) {
                builder.kraftMode(true);
                builder.kraftControllers(((KRaftCluster) annotation).numControllers());
            }
            if (annotation instanceof ZooKeeperCluster) {
                builder.kraftMode(false);
            }
            if (annotation instanceof SaslPlainAuth) {
                builder.saslMechanism("PLAIN");
                builder.securityProtocol("SASL_PLAINTEXT");
                builder.users(Arrays.stream(((SaslPlainAuth) annotation).value())
                        .collect(Collectors.toMap(
                                SaslPlainAuth.UserPassword::user,
                                SaslPlainAuth.UserPassword::password)));
            }
            if (annotation instanceof ClusterId) {
                builder.kafkaKraftClusterId(((ClusterId) annotation).value());
            }
            if (annotation instanceof BrokerConfig.List) {
                for (var config : ((BrokerConfig.List) annotation).value()) {
                    builder.brokerConfig(config.name(), config.value());
                }
            }
            else if (annotation instanceof BrokerConfig) {
                builder.brokerConfig(((BrokerConfig) annotation).name(), ((BrokerConfig) annotation).value());
            }
        }
        KafkaClusterConfig clusterConfig = builder.build();
        return clusterConfig;
    }

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
