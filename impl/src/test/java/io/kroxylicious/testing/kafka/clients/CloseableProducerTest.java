/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.clients;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Producer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
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
        return Arrays.stream(Producer.class.getMethods())
                .filter(method -> !Modifier.isStatic(method.getModifiers()) &&
                        !method.isSynthetic() &&
                        !method.getName().equals("close"));
    }

    @ParameterizedTest
    @MethodSource("delegatedMethods")
    void shouldDelegateMethod(Method method) throws Exception {
        // Given
        Object[] args = mockArgumentsForMethod(method);
        Object mockResult = null;
        if (method.getReturnType() != void.class) {
            Class<?> returnType = method.getReturnType();
            if (returnType == long.class) {
                mockResult = 1L;
            }
            else {
                mockResult = mock(returnType);
            }
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

    private static Object @NotNull [] mockArgumentsForMethod(Method method) {
        return Arrays.stream(method.getParameterTypes())
                .map(param -> {
                    if (param == String.class) {
                        return "test";
                    }
                    else if (param == byte[].class) {
                        return new byte[0];
                    }
                    else if (param == Map.class) {
                        return Map.of();
                    }
                    else if (param == Collection.class) {
                        return List.of();
                    }
                    else if (param == int.class) {
                        return 1;
                    }
                    else if (param == long.class) {
                        return 1L;
                    }
                    else if (param == Optional.class) {
                        return Optional.empty();
                    }
                    return mock(param);
                })
                .toArray();
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

}
