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

    public synchronized int getPort(Listener listener, int nodeId) {
        return ports.get(listener).get(nodeId).getLocalPort();
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

    public synchronized boolean containsPort(Listener listener, int nodeId) {
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

    public synchronized void deallocate(int nodeId) {
        for (var value : ports.values()) {
            value.remove(nodeId);
        }
    }

    /**
     * An allocation session where all ports allocated are unique within that session.
     */
    public class PortAllocationSession implements Closeable {
        ListeningSocketPreallocator preallocator = new ListeningSocketPreallocator();

        public void allocate(Set<Listener> listeners, int firstBrokerIdInclusive, int lastBrokerIdExclusive) {
            PortAllocator.this.allocate(listeners, firstBrokerIdInclusive, lastBrokerIdExclusive, preallocator);
        }

        public void allocate(Set<Listener> listeners, int brokerId) {
            PortAllocator.this.allocate(listeners, brokerId, brokerId + 1, preallocator);
        }

        @Override
        public void close() {
            preallocator.close();
        }
    }
}
