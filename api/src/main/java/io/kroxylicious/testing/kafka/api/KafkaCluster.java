/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.api;

import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * A KafkaCluster, from which is it possible to create/connect clients.
 * Clients can be created using {@link #getKafkaClientConfiguration()} or
 * {@link #getKafkaClientConfiguration(String, String)}.
 * <br/>
 * The cluster needs to be {@link #start() start}ed prior to use,
 * and should be {@link #close() close}d after use.
 */
public interface KafkaCluster extends AutoCloseable {
    /**
     * starts the cluster.
     */
    void start();

    /**
     * adds a new broker to the cluster. Once this method returns the caller is guaranteed that the new broker will
     * be incorporated into the cluster.  In kraft mode, the broker added will always from the role 'broker'.
     *
     * @return kafka <code>node.id</code> of the created broker.
     */
    int addBroker();

    /**
     * removes broker identified by the given <code>node.id</code> from the cluster.  Once this method returns the
     * caller is guaranteed that the broker has stopped and is no longer part of the cluster.
     * <br/>
     * It is not permitted to remove a broker from a cluster that has stopped brokers.
     * <br/>
     * In kraft mode, it is not permitted to remove a broker that is the controller mode.
     *
     * @param nodeId node to be removed from the cluster
     * @throws UnsupportedOperationException the <code>node.id</code> identifies a kraft controller
     * @throws IllegalArgumentException      the node identified by <code>node.id</code> does not exist.
     * @throws IllegalStateException         the cluster has stopped brokers.
     */
    void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException, IllegalStateException;

    /**
     * Stops the nodes(s) identified by the supplied predicate.  Once this method returns the
     * caller is guaranteed that the node(s) have been stopped.
     * <br/>
     * The caller specifies a <code>terminationStyle</code>.  If it is set {@link TerminationStyle#ABRUPT}, the kafka
     * node(s) will be abruptly killed, rather than undergoing a graceful shutdown. This may be useful to test cases
     * wishing to explore networking edge cases.  The implementation may ignore this parameter if the
     * implementation is unable to support the requested style of termination. In this case, the implementation
     * is free to use an alternative termination style instead.
     * <br/>
     * If a subset of nodes in the cluster are stopped, there's no guarantee that the cluster actually remains
     * usable. It is the caller's responsibility to ensure that Kafka internal topics (such as <code>__consumer_offsets</code>)
     * remain sufficiently replicated.  In the KRaft case, it is the caller's responsibility to ensure at least one
     * controller remains.
     * <br/>
     * Stopping a node that is already stopped has no effect.
     *
     * @param nodeIdPredicate  predicate that returns true if the node identified by the given nodeId should be restarted
     * @param terminationStyle the style of termination used to shut down the broker(s).
     */
    void stopNodes(IntPredicate nodeIdPredicate, TerminationStyle terminationStyle);

    /**
     * Starts node(s) identified by the supplied predicate.  Use this method to restart node(s)
     * previously stopped by {@link #stopNodes(IntPredicate, TerminationStyle)}.
     * <br/>
     * Starting a node that is already started has no effect.
     *
     * @param nodeIdPredicate  predicate that returns true if the node identified by the given nodeId should be restarted
     */
    void startNodes(IntPredicate nodeIdPredicate);

    /**
     * stops the cluster.
     */
    @Override
    void close() throws Exception;

    /**
     * Gets the number of brokers in the cluster, including any that are stopped, excluding any pure controller nodes.
     * @return the number of brokers in the cluster.
     */
    int getNumOfBrokers();

    /**
     * Returns the set of node ids that correspond to the brokers that are currently stopped.  The set
     * will be empty if no brokers are currently stopped.
     *
     * @return set of node ids.
     */
    Set<Integer> getStoppedBrokers();

    /**
     * Gets the bootstrap servers for this cluster
     * @return bootstrap servers
     */
    String getBootstrapServers();

    /**
     * Gets the cluster id
     * @return The cluster id for KRaft-based clusters, otherwise null;
     */
    String getClusterId();

    /**
     * Gets the kafka configuration for making connections to this cluster as required by the
     * {@code org.apache.kafka.clients.admin.AdminClient}, {@code org.apache.kafka.clients.producer.Producer} etc.
     * Details such the bootstrap and SASL configuration are provided automatically.
     * The returned map is guaranteed to be mutable and is unique to the caller.
     *
     * @return mutable configuration map
     */
    Map<String, Object> getKafkaClientConfiguration();

    /**
     * Gets the kafka configuration for making connections to this cluster as required by the
     * {@code org.apache.kafka.clients.admin.AdminClient}, {@code org.apache.kafka.clients.producer.Producer} etc.
     * Details such the bootstrap and SASL configuration are provided automatically.
     * The returned map is guaranteed to be mutable and is unique to the caller.
     *
     * @param user The user
     * @param password The password
     * @return mutable configuration map
     */
    Map<String, Object> getKafkaClientConfiguration(String user, String password);
}
