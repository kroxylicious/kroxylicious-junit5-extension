/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Allocates one or more groups listening sockets from the ephemeral port range.
 *
 * The caller may call #preAllocateListeningSockets any number of times.
 * The caller is guaranteed that all allocated port numbers are unique, even across invocations of #preAllocateListeningSockets.
 *
 * Before the caller attempts to bind, they must call #close.  Once close is called no further use of the socket
 * allocator is permitted.
 */
public class ListeningSocketPreallocator implements AutoCloseable {

    private static final int PORT_RANGE_LOW = 10_000;
    private static final int PORT_RANGE_HIGH = 30_000;
    private final List<ServerSocket> all = new ArrayList<>();

    private final Random random = new Random();

    /**
     * Instantiates a new Listening socket preallocator.
     */
    public ListeningSocketPreallocator() {
    }

    /**
     * Pre-allocate 1 or more ports from A defined range, so ast to avoid collisions without going network connections,
     * which are available for use once the #close method is called.
     *
     * @param num number of ports to pre-allocate
     * @return stream of ephemeral ports
     */
    public Stream<ServerSocket> preAllocateListeningSockets(int num) {
        if (num < 1) {
            return Stream.of();
        }
        if (num > PORT_RANGE_HIGH - PORT_RANGE_LOW) {
            throw new IllegalArgumentException("Can't request more than " + (PORT_RANGE_HIGH - PORT_RANGE_LOW) + " ports");
        }
        final var allocated = new AtomicInteger(0);
        return random.ints(PORT_RANGE_LOW, PORT_RANGE_HIGH)
                .mapToObj(number -> {
                    try {
                        var serverSocket = new ServerSocket(number);
                        serverSocket.setReuseAddress(true);
                        if (serverSocket.isBound()) {
                            return serverSocket;
                        }
                    }
                    catch (IOException e) {
                        if (e instanceof BindException) {
                            return null;
                        }
                        else {
                            throw new RuntimeException(e);
                        }
                    }
                    return null;
                }).filter(Objects::nonNull)
                .takeWhile(serverSocket -> allocated.incrementAndGet() <= num)
                .peek(all::add);
    }

    /**
     * Pre-allocate 1 or more ephemeral ports which are available for use once the #close method is called.
     *
     * @param num number of ports to pre-allocate
     * @return stream of ephemeral ports
     */
    public Stream<ServerSocket> preAllocateEphemeralListeningSockets(int num) {
        if (num < 1) {
            return Stream.of();
        }

        var ports = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < num; i++) {
                try {
                    var serverSocket = new ServerSocket(0);
                    ports.add(serverSocket);
                    serverSocket.setReuseAddress(true);
                }
                catch (IOException e) {
                    System.getLogger("portAllocator").log(System.Logger.Level.WARNING, "failed to allocate port: ", e);
                    throw new UncheckedIOException(e);
                }

            }
        }
        finally {
            all.addAll(ports);
        }
        return ports.stream();
    }

    @Override
    public void close() {
        all.forEach(serverSocket -> {
            try {
                serverSocket.close();
            }
            catch (IOException e) {
                System.getLogger("portAllocator").log(System.Logger.Level.WARNING, "failed to release socket: ", e);
                throw new UncheckedIOException(e);
            }
        });
    }
}
