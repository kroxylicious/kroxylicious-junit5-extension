/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;

/**
 * Annotation constraining a {@link KafkaClusterProvisioningStrategy} to use
 * a {@link KafkaCluster} that is KRaft-based.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface KRaftCluster {
    /**
     * The number of kraft controllers
     * The extension will ensure there are enough nodes started with the <code>controller</code> role.
     * The extension will combine this with the <code>numBrokers</code> to generate a cluster topology.
     * <table>
     *  <tr><th>numBrokers</th><th>numControllers</th><th>roles</th></tr>
     *  <tr><td>1</td><td>1</td><td><code>"broker,controller"</code></td></tr>
     *  <tr><td>3</td><td>1</td><td><code>"broker,controller"</code>, <code>"broker"</code>, <code>"broker"</code></td></tr>
     *  <tr><td>1</td><td>3</td><td><code>"broker,controller"</code>, <code>"controller"</code>, <code>"controller"</code></td></tr>
     * </table>
     * @return The number of KRaft controllers
     */
    public int numControllers() default 1;

}
