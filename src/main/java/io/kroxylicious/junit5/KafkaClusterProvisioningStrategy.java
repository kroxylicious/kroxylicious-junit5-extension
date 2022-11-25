/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.jupiter.api.extension.ExtensionConfigurationException;

import io.kroxylicious.cluster.KafkaCluster;
import io.kroxylicious.cluster.KafkaClusterConfig;
import io.kroxylicious.junit5.constraint.BrokerCluster;
import io.kroxylicious.junit5.constraint.BrokerConfig;
import io.kroxylicious.junit5.constraint.ClusterId;
import io.kroxylicious.junit5.constraint.KRaftCluster;
import io.kroxylicious.junit5.constraint.SaslPlainAuth;
import io.kroxylicious.junit5.constraint.ZooKeeperCluster;

import static java.lang.System.Logger.Level.TRACE;

public interface KafkaClusterProvisioningStrategy {

    public static KafkaClusterConfig kafkaClusterConfig(AnnotatedElement sourceElement) {
        System.Logger logger = System.getLogger(KafkaClusterProvisioningStrategy.class.getName());
        var builder = KafkaClusterConfig.builder();

        if (sourceElement.isAnnotationPresent(BrokerCluster.class)) {
            builder.brokersNum(sourceElement.getAnnotation(BrokerCluster.class).numBrokers());
        }
        else {
            builder.brokersNum(1);
        }
        if (sourceElement.isAnnotationPresent(KRaftCluster.class)
                && sourceElement.isAnnotationPresent(ZooKeeperCluster.class)) {
            throw new ExtensionConfigurationException(
                    "Either @" + KRaftCluster.class.getSimpleName() + " or @" + ZooKeeperCluster.class.getSimpleName() + " can be used, not both");
        }
        else if (sourceElement.isAnnotationPresent(KRaftCluster.class)) {
            var kraft = sourceElement.getAnnotation(KRaftCluster.class);
            builder.kraftMode(true)
                    .kraftControllers(kraft.numControllers());
        }
        else if (sourceElement.isAnnotationPresent(ZooKeeperCluster.class)) {
            builder.kraftMode(false);
            if (sourceElement.isAnnotationPresent(ClusterId.class)
                    && !sourceElement.getAnnotation(ClusterId.class).value().isEmpty()) {
                throw new ExtensionConfigurationException("Specifying @" + ClusterId.class.getSimpleName() + " with @" +
                        ZooKeeperCluster.class.getSimpleName() + " is not supported");
            }
        }
        else {
            builder.kraftMode(true).kraftControllers(1);
        }

        if (sourceElement.isAnnotationPresent(SaslPlainAuth.class)) {
            var authn = sourceElement.getAnnotation(SaslPlainAuth.class);
            builder.saslMechanism("PLAIN");
            builder.users(Arrays.stream(authn.value())
                    .collect(Collectors.toMap(
                            SaslPlainAuth.UserPassword::user,
                            SaslPlainAuth.UserPassword::password)));
        }
        if (sourceElement.isAnnotationPresent(ClusterId.class)
                && !sourceElement.getAnnotation(ClusterId.class).value().isEmpty()) {
            builder.kafkaKraftClusterId(sourceElement.getAnnotation(ClusterId.class).value());
        }

        if (sourceElement.isAnnotationPresent(BrokerConfig.class)
                || sourceElement.isAnnotationPresent(BrokerConfig.BrokerConfigs.class)) {
            for (var config : sourceElement.getAnnotationsByType(BrokerConfig.class)) {
                logger.log(TRACE, "decl {0}: Setting broker config {1}={2}", sourceElement, config.name(), config.value());
                builder.brokerConfig(config.name(), config.value());
            }
        }
        else {
            logger.log(TRACE, "decl {0}: No broker configs", sourceElement);
        }
        KafkaClusterConfig clusterConfig = builder.build();
        logger.log(TRACE, "decl {0}: Using config {1}", sourceElement, clusterConfig);
        return clusterConfig;
    }

    // This implies that the extension knows how to create a config from the annotations
    // which implies hard-coded annotations
    // We actually need to know:
    // a. Which annotations are constraints (meta-annotation)
    // b. Find all provisioning strategies which support all the annotations on the decl
    // c. Filter for decl type
    // d. Move the creation of config from annotations into the strategy
    boolean supportsAnnotation(Class<? extends Annotation> constraint);
    // TODO this ^^ doesn't cope with the possibility that it's the combination of
    // constraints that's the problem
    // But having a per-constraint method is helpful for debugging
    // why a provisioner got ruled out
    // To fix that create() should be allowed to throw or otherwise express the inability
    // to actually consume the whole config.

    boolean supportsType(Class<? extends KafkaCluster> declarationType);

    KafkaCluster create(AnnotatedElement sourceElement,
                        Class<? extends KafkaCluster> declarationType);

    // TODO logically the time depends on the configuration
    float estimatedProvisioningTimeMs();

}
