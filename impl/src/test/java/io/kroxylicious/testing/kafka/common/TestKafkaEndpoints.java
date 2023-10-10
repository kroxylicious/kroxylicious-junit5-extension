/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.jetbrains.annotations.NotNull;

public class TestKafkaEndpoints implements KafkaEndpoints {

    public static final int CLIENT_BASE_PORT = 9092;
    public static final int CONTROLLER_BASE_PORT = 10092;
    public static final int INTER_BROKER_BASE_PORT = 11092;
    public static final int ANON_BASE_PORT = 12092;

    @NotNull
    private static EndpointPair generateEndpoint(int nodeId, int basePort) {
        final int port = basePort + nodeId;
        return new EndpointPair(new Endpoint("0.0.0.0", port), new Endpoint("localhost", port));
    }

    @Override
    public EndpointPair getEndpointPair(Listener listener, int nodeId) {
        switch (listener) {

            case EXTERNAL -> {
                return generateEndpoint(nodeId, CLIENT_BASE_PORT);
            }
            case ANON -> {
                return generateEndpoint(nodeId, ANON_BASE_PORT);
            }
            case INTERNAL -> {
                return generateEndpoint(nodeId, INTER_BROKER_BASE_PORT);
            }
            case CONTROLLER -> {
                return generateEndpoint(nodeId, CONTROLLER_BASE_PORT);
            }

            default -> throw new IllegalStateException("Unexpected value: " + listener);
        }
    }
}
