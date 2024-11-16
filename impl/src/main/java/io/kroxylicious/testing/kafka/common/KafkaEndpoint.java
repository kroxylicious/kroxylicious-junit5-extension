/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type Endpoint.
 *
 * @param host the host
 * @param port the port
 */
public record KafkaEndpoint(@NonNull String host, int port) {
    public KafkaEndpoint {
        Objects.requireNonNull(host);
    }

    /**
     * kafka formatted address suitable for use in configuration.
     * @return kafka formatted address suitable for use in configuration.
     */
    @Override
    public String toString() {
        return host + ":" + port;
    }

}
