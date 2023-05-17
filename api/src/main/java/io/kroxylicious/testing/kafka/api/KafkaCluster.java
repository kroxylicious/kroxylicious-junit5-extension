/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.api;

import java.util.Map;

/**
 * A KafkaCluster, from which is it possible to create/connect clients.
 * Clients can be created using {@link #getKafkaClientConfiguration()} or
 * {@link #getKafkaClientConfiguration(String, String)}.
 *
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
     * <p>
     * in kraft mode, it is not permitted to remove a broker that is the controller mode.
     *
     * @param nodeId node to be removed from the cluster
     * @throws UnsupportedOperationException the <code>node.id</code> identifies a kraft controller
     * @throws IllegalArgumentException      the node identified by <code>node.id</code> does not exist.
     */
    void removeBroker(int nodeId) throws UnsupportedOperationException, IllegalArgumentException;

    /**
     * stops the cluster.
     */
    @Override
    void close() throws Exception;

    /**
     * Gets the number of brokers expected in the cluster
     * @return the size of the cluster.
     */
    int getNumOfBrokers();

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
