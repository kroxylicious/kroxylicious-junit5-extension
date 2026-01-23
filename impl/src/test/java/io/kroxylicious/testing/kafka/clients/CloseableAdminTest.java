/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.clients;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
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
class CloseableAdminTest {

    @Mock
    private Admin delegate;

    private CloseableAdmin closeableAdmin;

    @BeforeEach
    void setUp() {
        closeableAdmin = new CloseableAdmin(delegate);
    }

    static Stream<Method> delegatedMethods() {
        return MethodUtils.delegatedMethods(Admin.class);
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
        Object actualResult = method.invoke(closeableAdmin, args);

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
        closeableAdmin.close();
        verify(delegate).close(Duration.ofSeconds(5L));
    }

    @Test
    void shouldDelegateCloseWithSpecificTimeout() {
        var timeout = Duration.ofMinutes(1);
        closeableAdmin.close(timeout);
        verify(delegate).close(timeout);
    }

}
