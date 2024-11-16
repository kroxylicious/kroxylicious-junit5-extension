/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaListenerTest {

    @Test
    void constructorValidateArguments() {
        var endpoint = new KafkaEndpoint("host", 1);
        assertThatThrownBy(() -> new KafkaListener(null, endpoint, endpoint))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaListener(endpoint, null, endpoint))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new KafkaListener(endpoint, endpoint, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void buildListenerForContainer() {
        var listener = KafkaListener.build(1234, "0.0.0.0", "container", 4321, "host");
        assertThat(listener)
                .returns(new KafkaEndpoint("0.0.0.0", 1234), KafkaListener::bind)
                .returns(new KafkaEndpoint("container", 1234), KafkaListener::kafkaNet)
                .returns(new KafkaEndpoint("host", 4321), KafkaListener::advertised);
    }

    @Test
    void buildListenerForInVM() {
        var listener = KafkaListener.build(1234, "0.0.0.0", "localhost");
        assertThat(listener)
                .returns(new KafkaEndpoint("0.0.0.0", 1234), KafkaListener::bind)
                .returns(new KafkaEndpoint("localhost", 1234), KafkaListener::kafkaNet)
                .returns(new KafkaEndpoint("localhost", 1234), KafkaListener::advertised);
    }

}