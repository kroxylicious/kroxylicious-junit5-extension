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

/**
 * {@code @BrokerCluster} can be used to annotate a field in a test class or a
 *  parameter in a lifecycle method or test method of type {@link KafkaCluster}
 *  that should be resolved into a temporary Kafka cluster.
 *
 * <p>A simple example looks like:</p>
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class MyTest {
 *     @Test
 *     public void testKafkaClusterParameter(
 *             @BrokerCluster(numBrokers = 3) KafkaCluster cluster) {
 *         //... your test code using `cluster`
 *     }
 * }
 * }</pre>
 *
 * <p>Alternatively annotate a {@code static} or member field:</p>
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class MyTest {
 *     @BrokerCluster(numBrokers = 3) KafkaCluster cluster;
 *     @Test
 *     public void testKafkaClusterParameter() {
 *         //... your test code using `cluster`
 *     }
 * }
 * }</pre>
 *
 * <p>By default a KRaft cluster with a single broker and colocated controller
 * will be used using containers.
 * However, the configuration of the cluster
 * can be influenced with a number of other annotations:</p>
 * <dl>
 *     <dt>{@link KRaftCluster}</dt><dd>Allow specifying the number of KRaft controllers</dd>
 *     <dt>{@link ZooKeeperCluster}</dt><dd>Allow specifying that a ZK-based cluster should be used</dd>
 *     <dt>{@link SaslPlainAuth}</dt><dd>Will configure the cluster for SASL-PLAIN authentication</dd>
 * </dl>
 *
 * <p>For example:</p>
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class MyTest {
 *     @Test
 *     public void testKafkaClusterParameter(
 *             @BrokerCluster(numBrokers = 3)
 *             @KRaftCluster(numControllers = 3)
 *             @SaslPlainAuth({
 *                 @UserPassword(user="alice", password="foo"),
 *                 @UserPassword(user="bob", password="bar")
 *             })
 *             KafkaCluster cluster) {
 *         //... your test code using `cluster`
 *     }
 * }
 * }</pre>
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@KafkaClusterConstraint
public @interface BrokerCluster {
    /**
     * Number of brokers in the cluster
     * The extension will combine this with the <code>numControllers</code> when in Kraft mode to generate a cluster topology.
     * <table>
     *  <caption>Breakdown of the interaction between numControllers and numBrokers</caption>
     *  <tr><th>numBrokers</th><th>numControllers</th><th>roles</th></tr>
     *  <tr><td>1</td><td>1</td><td><code>"broker,controller"</code></td></tr>
     *  <tr><td>3</td><td>1</td><td><code>"broker,controller"</code>, <code>"broker"</code>, <code>"broker"</code></td></tr>
     *  <tr><td>1</td><td>3</td><td><code>"broker,controller"</code>, <code>"controller"</code>, <code>"controller"</code></td></tr>
     *  <tr><td>3</td><td>3</td><td><code>"broker,controller"</code>, <code>"broker,controller"</code>, <code>"broker,controller"</code></td></tr>
     * </table>
     * @return The number of brokers in the cluster
     */
    // TODO should this be minBrokers?
    int numBrokers() default 1;
}
