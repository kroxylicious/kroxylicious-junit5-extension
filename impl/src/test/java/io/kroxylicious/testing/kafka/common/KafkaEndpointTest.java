/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaEndpointTest {
    @Test
    void requiresHostNotNull() {
        assertThatThrownBy(() -> new KafkaEndpoint(null, 1234))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void toStringFormatAddress() {
        var ep = new KafkaEndpoint("foo", 1234);
        assertThat(ep).hasToString("foo:1234");
    }

}