/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ListeningSocketPreallocatorTest {

    @Test
    void preallocateOne() {
        var preallocator = new ListeningSocketPreallocator();
        var sock = preallocator.preAllocateListeningSockets(1).findFirst();
        assertThat(sock).isNotEmpty();
        assertThat(sock.get().isClosed()).isFalse();
        preallocator.close();
        assertThat(sock.get().isClosed()).isTrue();
    }
}