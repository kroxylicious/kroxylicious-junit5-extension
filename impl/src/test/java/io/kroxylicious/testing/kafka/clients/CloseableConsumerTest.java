/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.clients;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
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
class CloseableConsumerTest {

    @Mock
    private Consumer<?, ?> delegate;

    private CloseableConsumer<?, ?> closeableConsumer;

    @BeforeEach
    void setUp() {
        closeableConsumer = new CloseableConsumer<>(delegate);
    }

    static Stream<Method> delegatedMethods() {
        return MethodUtils.delegatedMethods(Consumer.class);
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
        Object actualResult = method.invoke(closeableConsumer, args);

        // Then
        method.invoke(verify(delegate), args);
        if (mockResult != null) {
            assertThat(actualResult).isEqualTo(mockResult);
        }
        else {
            assertThat(actualResult).isNull();
        }
    }

    @Test
    void shouldDelegateCloseWithDefaultTimeout() {
        closeableConsumer.close();
        verify(delegate).close(Duration.of(5, ChronoUnit.SECONDS));
    }

    @Test
    void shouldDelegateCloseWithSpecificTimeout() {
        var timeout = Duration.ofMinutes(1);
        closeableConsumer.close(timeout);
        verify(delegate).close(timeout);
    }

}
