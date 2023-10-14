/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import lombok.Builder;
import lombok.Getter;

/**
 * The interface Kafka endpoints.
 */
public interface KafkaEndpoints {

    /**
     * The type Endpoint pair.
     */
    @Builder
    @Getter
    class EndpointPair {
        private final Endpoint bind;
        private final Endpoint connect;

        /**
         * Instantiates a new Endpoint pair.
         *
         * @param bind    the bind
         * @param connect the endpoint
         */
        public EndpointPair(Endpoint bind, Endpoint connect) {
            this.bind = bind;
            this.connect = connect;
        }

        /**
         * Connect address string.
         *
         * @return the address
         */
        public String connectAddress() {
            return String.format("%s:%d", connect.host, connect.port);
        }

        /**
         * Listen address string.
         *
         * @return the listen address
         */
        public String listenAddress() {
            return String.format("//%s:%d", bind.host, bind.port);
        }

        /**
         * Advertised address string.
         *
         * @return the advertise address
         */
        public String advertisedAddress() {
            return String.format("//%s:%d", connect.host, connect.port);
        }
    }

    /**
     * The type Endpoint.
     */
    @Builder
    @Getter
    class Endpoint {
        private final String host;
        private final int port;

        /**
         * Instantiates a new Endpoint.
         *
         * @param host the host
         * @param port the port
         */
        public Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return String.format("//%s:%d", host, port);
        }
    }

    /**
     * Gets the endpoint for the given listener and brokerId.
     *
     * @param listener listener
     * @param nodeId   kafka <code>node.id</code>
     * @return endpoint poir.
     */
    EndpointPair getEndpointPair(Listener listener, int nodeId);

}
