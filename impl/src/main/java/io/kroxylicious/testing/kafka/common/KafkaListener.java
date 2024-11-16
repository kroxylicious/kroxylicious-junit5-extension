/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type Kafka Listener.
 *
 * @param bind       the bind address
 * @param kafkaNet   the connect address within the kafka network
 * @param advertised the connect address from the client network
 */
public record KafkaListener(@NonNull KafkaEndpoint bind, @NonNull KafkaEndpoint kafkaNet, @NonNull KafkaEndpoint advertised) {

    public KafkaListener {
        Objects.requireNonNull(bind);
        Objects.requireNonNull(kafkaNet);
        Objects.requireNonNull(advertised);
    }

    public static KafkaListener build(int port, String bindAddress, String host) {
        var bind = new KafkaEndpoint(bindAddress, port);
        var advertised = new KafkaEndpoint(host, port);
        return new KafkaListener(bind, advertised, advertised);
    }

    public static KafkaListener build(int containerPort, String bindAddress, String containerHost, int externalPort, String clientHost) {
        var bind = new KafkaEndpoint(bindAddress, containerPort);
        var container = new KafkaEndpoint(containerHost, containerPort);
        var advertised = new KafkaEndpoint(clientHost, externalPort);
        return new KafkaListener(bind, container, advertised);
    }

}
