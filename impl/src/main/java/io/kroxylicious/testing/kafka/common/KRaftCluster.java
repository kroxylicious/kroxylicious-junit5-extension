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
 *
 * <table>
 *  <caption>Breakdown of the interaction between numControllers and numBrokers</caption>
 *  <tr><th>numBrokers</th><th>numControllers</th><th>combinedMode</th><th>roles</th></tr>
 *  <tr><td>1</td><td>1</td><td>true</td><td>1×<code>"broker,controller"</code></td></tr>
 *  <tr><td>1</td><td>1</td><td>false</td><td>1×<code>"broker"</code>, 1×<code>"controller"</code></td></tr>
 *
 *  <tr><td>3</td><td>1</td><td>true</td><td>1×<code>"broker,controller"</code>, 2×<code>"broker"</code></td></tr>
 *  <tr><td>3</td><td>1</td><td>false</td><td>3×<code>"broker"</code>, 1×<code>"controller"</code>, </td></tr>
 *
 *  <tr><td>1</td><td>3</td><td>true</td><td>1×<code>"broker,controller"</code>, 2×<code>"controller"</code></td></tr>
 *  <tr><td>1</td><td>3</td><td>false</td><td>1×<code>"broker"</code>, 3×<code>"controller"</code></td></tr>
 *
 *  <tr><td>3</td><td>3</td><td>true</td><td>3×<code>"broker,controller"</code></td></tr>
 *  <tr><td>3</td><td>3</td><td>true</td><td>3×<code>"broker"</code>, 3×<code>"controller"</code></td></tr>
 * </table>
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface KRaftCluster {
    /**
     * The number of kraft controllers.
     * The extension will ensure there are this many nodes started with the <code>controller</code> role.
     * combining this with the <code>numBrokers</code> and <code>combinedMode</code> to generate a cluster topology.
     *
     * See the class JavaDoc for example topologies.
     * @return The number of KRaft controllers
     */
    public int numControllers() default 1;

    /**
     * Whether to use combined mode, where controllers can share a JVM with brokers.
     * See the class JavaDoc for example topologies.
     * @return true to use combined mode.
     */
    public boolean combinedMode() default true;

}
