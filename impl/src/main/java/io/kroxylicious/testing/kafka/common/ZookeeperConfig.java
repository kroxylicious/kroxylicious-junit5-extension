/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

public class ZookeeperConfig {
    private final KafkaEndpoints.EndpointPair endpoint;

    public ZookeeperConfig(KafkaEndpoints endpoints) {
        this.endpoint = endpoints.getEndpointPair(Listener.CONTROLLER, 0);
    }

    public int getPort() {
        return endpoint.getBind().getPort();
    }

    public String connectAddress() {
        return endpoint.connectAddress();
    }
}
