/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.common;

import java.io.Closeable;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kroxylicious.testing.kafka.common.KafkaClusterConfig.KafkaEndpoints.Listener;

/**
 * Allocates ports to a <code>listener</code> and <code>node.id</code> tuple.
 */
public class PortAllocator {

    /**
     * Tracks ports that are in-use by listener.
     * The inner map is a mapping of kafka <code>node.id</code> to a closed server socket, with a port number
     * previously defined from the ephemeral range.
     * Protected by lock of {@link PortAllocator itself.}
     */
    private final Map<Listener, Map<Integer, ServerSocket>> ports;

    public PortAllocator() {
        ports = new HashMap<>();
    }

    /**
     * Gets the previously allocated port for the given <code>listener</code> and <code>node.id</code> tuple.
     *
     * @param listener listener
     * @param nodeId node id
     * @throws IllegalArgumentException if there is no allocated port for the given tuple.
     * @return port number
     */
    public synchronized int getPort(Listener listener, int nodeId) {
        if (!hasRegisteredPort(listener, nodeId)) {
            throw new IllegalArgumentException("listener " + listener + " does not have a port for node " + nodeId + ".");
        }
        return ports.get(listener).get(nodeId).getLocalPort();
    }

    /**
     * Tests whether there is a port allocated for the given <code>listener</code> and <code>node.id</code> tuple.
     * @param listener listener
     * @param nodeId node id
     * @return true if there is a port assigned to the tuple, false otherwise.
     */
    public synchronized boolean hasRegisteredPort(Listener listener, int nodeId) {
        Map<Integer, ServerSocket> portsMap = ports.get(listener);
        return portsMap != null && portsMap.containsKey(nodeId);
    }

    /**
     * Since port pre-allocation is stateful (ensuring all ports assigned during pre-allocation
     * are unique), we need to keep a reference to that pre-allocator while we are allocating all
     * the initial ports for a cluster
     *
     * @return A session which holds the allocated ports open until it is closed
     */
    public PortAllocationSession allocationSession() {
        return new PortAllocationSession();
    }

    /**
     * De-allocates all ports for the given <code>nodeId</code>.
     * @param nodeId node id.
     */
    public synchronized void deallocate(int nodeId) {
        for (var value : ports.values()) {
            value.remove(nodeId);
        }
    }

    private synchronized void allocate(Set<Listener> listeners, int firstBrokerIdInclusive, int lastBrokerIdExclusive, ListeningSocketPreallocator preallocator) {
        if (lastBrokerIdExclusive <= firstBrokerIdInclusive) {
            throw new IllegalArgumentException(
                    "attempted to allocate ports to an invalid range of broker ids: [" + firstBrokerIdInclusive + "," + lastBrokerIdExclusive + ")");
        }
        int toAllocate = lastBrokerIdExclusive - firstBrokerIdInclusive;
        for (Listener listener : listeners) {
            List<ServerSocket> sockets = preallocator.preAllocateListeningSockets(toAllocate);
            Map<Integer, ServerSocket> listenerPorts = ports.computeIfAbsent(listener, listener1 -> new HashMap<>());
            for (int i = 0; i < toAllocate; i++) {
                listenerPorts.put(firstBrokerIdInclusive + i, sockets.get(i));
            }
        }
    }

    /**
     * An allocation session where all ports allocated are unique within that session.
     */
    public class PortAllocationSession implements Closeable {
        ListeningSocketPreallocator preallocator = new ListeningSocketPreallocator();

        /**
         * Allocates a set of ports for the given range of node ids.
         *
         * @param listeners set of listeners
         * @param firstNodeIdInclusive first node id (inclusive)
         * @param lastNodeIdExclusive last node id (exclusive)
         */
        public void allocate(Set<Listener> listeners, int firstNodeIdInclusive, int lastNodeIdExclusive) {
            PortAllocator.this.allocate(listeners, firstNodeIdInclusive, lastNodeIdExclusive, preallocator);
        }

        /**
         * Allocates a set of ports for the given node id.
         *
         * @param listeners set of listeners
         * @param nodeId node id
         */
        public void allocate(Set<Listener> listeners, int nodeId) {
            PortAllocator.this.allocate(listeners, nodeId, nodeId + 1, preallocator);
        }

        /**
         * Ends the allocation session.  This will make the  ports allocate available for use.
         */
        @Override
        public void close() {
            preallocator.close();
        }
    }
}
