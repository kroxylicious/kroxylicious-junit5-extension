/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.clients;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.mockito.Mockito.mock;

final class MethodUtils {
    private MethodUtils() {
        // do not construct
    }

    static Stream<Method> delegatedMethods(Class<?> adminClass) {
        return Arrays.stream(adminClass.getMethods())
                .filter(method -> !Modifier.isStatic(method.getModifiers()) &&
                        !method.isSynthetic() &&
                        !method.getName().equals("close"));
    }

    static Object[] mockArgumentsForMethod(Method method) {
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

    public static @Nullable Object buildMockResultsForMethod(Method method) {
        if (method.getReturnType() != void.class) {
            Class<?> returnType = method.getReturnType();
            if (returnType == long.class) {
                return 1L;
            }
            else {
                return mock(returnType);
            }
        }
        return null;
    }
}
