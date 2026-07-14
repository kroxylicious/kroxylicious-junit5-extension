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

    /**
     * Creates a KafkaListener, validating that all endpoint arguments are non-null.
     */
    public KafkaListener {
        Objects.requireNonNull(bind);
        Objects.requireNonNull(kafkaNet);
        Objects.requireNonNull(advertised);
    }

    /**
     * Builds a simple listener where the bind address, intra-cluster address and advertised address all use the same port.
     *
     * @param port        the port to listen on
     * @param bindAddress the local bind address (e.g. {@code "0.0.0.0"})
     * @param host        the hostname to advertise to clients and peers
     * @return the constructed listener
     */
    public static KafkaListener build(int port, String bindAddress, String host) {
        var bind = new KafkaEndpoint(bindAddress, port);
        var advertised = new KafkaEndpoint(host, port);
        return new KafkaListener(bind, advertised, advertised);
    }

    /**
     * Builds a listener for a containerised broker where the container port and external port differ.
     *
     * @param containerPort the port inside the container
     * @param bindAddress   the local bind address inside the container (e.g. {@code "0.0.0.0"})
     * @param containerHost the hostname of the container on the Docker network
     * @param externalPort  the port mapped on the host
     * @param clientHost    the hostname that external clients use to reach the broker
     * @return the constructed listener
     */
    public static KafkaListener build(int containerPort, String bindAddress, String containerHost, int externalPort, String clientHost) {
        var bind = new KafkaEndpoint(bindAddress, containerPort);
        var container = new KafkaEndpoint(containerHost, containerPort);
        var advertised = new KafkaEndpoint(clientHost, externalPort);
        return new KafkaListener(bind, container, advertised);
    }

}
