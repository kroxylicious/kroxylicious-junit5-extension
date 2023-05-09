/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.net.ServerSocket;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class ListeningSocketPreallocatorTest {

    private ListeningSocketPreallocator preallocator;

    @BeforeEach
    void setUp() {
        preallocator = new ListeningSocketPreallocator();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 5, 100, 10_000 })
    void shouldAllocateOpenSockets(int numPorts) {
        var sockets = preallocator.preAllocateListeningSockets(numPorts);
        assertThat(sockets).hasSize(numPorts);
        assertThat(sockets).allSatisfy(entry -> assertThat(entry.isClosed()).isFalse());

        preallocator.close();

        assertThat(sockets).allSatisfy(entry -> assertThat(entry.isClosed()).isTrue());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 5, 100, 10_000 })
    void shouldCreateDistinctPorts(int numPorts) {
        // Given

        // When
        var sockets = preallocator.preAllocateListeningSockets(numPorts);

        // Then
        final Set<Integer> localPorts = sockets.stream().map(ServerSocket::getLocalPort).collect(Collectors.toSet());
        assertThat(localPorts).hasSize(numPorts);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    void shouldCreateDistinctPortsAcrossMultipleInvocations(int numAllocations) {
        // Given
        final int portPerInvocation = 5;

        // When
        final Set<Integer> localPorts = IntStream.rangeClosed(1, numAllocations)
                .boxed()
                .flatMap(idx -> preallocator.preAllocateListeningSockets(portPerInvocation).stream())
                .map(ServerSocket::getLocalPort)
                .collect(Collectors.toSet());

        // Then
        assertThat(localPorts).hasSize(numAllocations * portPerInvocation);
    }

    @AfterEach
    void tearDown() {
        preallocator.close();
    }
}