/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.NonNull;

class ReflectionUtils {

    private ReflectionUtils() {
    }

    private static final Map<Type, Class<?>> primitiveToBox = Map.of(
            Boolean.TYPE, Boolean.class,
            Byte.TYPE, Byte.class,
            Character.TYPE, Character.class,
            Short.TYPE, Short.class,
            Integer.TYPE, Integer.class,
            Long.TYPE, Long.class,
            Double.TYPE, Double.class,
            Float.TYPE, Float.class,
            Void.TYPE, Void.TYPE);

    @NonNull
    @SuppressWarnings("unchecked")
    public static <K> Optional<K> construct(Class<K> clazz, Object... parameters) {
        Constructor<?>[] declaredConstructors = clazz.getDeclaredConstructors();
        return Arrays.stream(declaredConstructors)
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .filter(constructor -> {
                    if (constructor.getParameterCount() != parameters.length) {
                        return false;
                    }
                    boolean allMatch = true;
                    Class<?>[] parameterTypes = constructor.getParameterTypes();
                    for (int i = 0; i < parameters.length; i++) {
                        allMatch = allMatch && (parameterTypes[i].isAssignableFrom(parameters[i].getClass())
                                || (parameterTypes[i].isPrimitive() && primitiveToBox.get(parameterTypes[i]).isAssignableFrom(parameters[i].getClass())));
                    }
                    return allMatch;
                }).findFirst().map(constructor -> {
                    try {
                        return (K) constructor.newInstance(parameters);
                    }
                    catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
