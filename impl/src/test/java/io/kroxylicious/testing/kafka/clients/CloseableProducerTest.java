/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.clients;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloseableProducerTest {

    @Mock
    private Producer<?, ?> delegate;

    private CloseableProducer<?, ?> closeableProducer;

    @BeforeEach
    void setUp() {
        closeableProducer = new CloseableProducer<>(delegate);
    }

    static Stream<Method> delegatedMethods() {
        return MethodUtils.delegatedMethods(Producer.class);
    }

    @ParameterizedTest
    @MethodSource("delegatedMethods")
    void shouldDelegateMethod(Method method) throws Exception {
        // Given
        Object[] args = MethodUtils.mockArgumentsForMethod(method);
        Object mockResult = MethodUtils.buildMockResultsForMethod(method);
        if (mockResult != null) {
            when(method.invoke(delegate, args)).thenReturn(mockResult);
        }

        // When
        Object actualResult = method.invoke(closeableProducer, args);

        // Then
        method.invoke(verify(delegate), args);
        if (mockResult != null) {
            assertThat(actualResult).isSameAs(mockResult);
        }
        else {
            assertThat(actualResult).isNull();
        }
    }

    @Test
    void shouldDelegateCloseWithDefaultTimeout() {
        closeableProducer.close();
        verify(delegate).close(Duration.ofSeconds(5L));
    }

    @Test
    void shouldDelegateCloseWithSpecificTimeout() {
        var timeout = Duration.ofMinutes(1);
        closeableProducer.close(timeout);
        verify(delegate).close(timeout);
    }

    @Test
    void wrapShouldReturnWrappedInstance() {
        // When
        Producer<?, ?> wrapped = CloseableProducer.wrap(delegate);

        // Then
        assertThat(wrapped).isInstanceOf(CloseableProducer.class);
        assertThat(((CloseableProducer<?, ?>) wrapped).instance()).isSameAs(delegate);
    }

    @Test
    void createWithPropertiesShouldReturnCloseableInstance() {
        // Given
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // When / Then
        try (Producer<String, String> producer = CloseableProducer.create(properties)) {
            assertThat(producer).isInstanceOf(CloseableProducer.class)
                    .isInstanceOf(AutoCloseable.class);
        }
    }

    @Test
    void createWithMapShouldReturnCloseableInstance() {
        // Given
        Map<String, Object> configs = Map.of(
                "bootstrap.servers", "localhost:9092",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // When / Then
        try (Producer<String, String> producer = CloseableProducer.create(configs)) {
            assertThat(producer).isInstanceOf(CloseableProducer.class)
                    .isInstanceOf(AutoCloseable.class);
        }
    }

}
