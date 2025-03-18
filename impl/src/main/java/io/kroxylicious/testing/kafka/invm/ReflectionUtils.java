/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

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
                .filter(constructor -> matchingMethod(constructor.getParameterTypes(), parameters))
                .findFirst().map(constructor -> {
                    try {
                        return (K) constructor.newInstance(parameters);
                    }
                    catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    public static <R> R invokeInstanceMethod(@NonNull Object target, @NonNull String methodName, Object... parameters) {
        var methods = target.getClass().getMethods();
        return invokeMethod(target.getClass(), target, methodName, parameters, methods,
                method -> Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers()));
    }

    public static <R> R invokeStaticMethod(@NonNull Class<?> clazz, @NonNull String methodName, Object... parameters) {
        var methods = clazz.getDeclaredMethods();
        return invokeMethod(clazz, null, methodName, parameters, methods,
                method -> Modifier.isPublic(method.getModifiers()) && Modifier.isStatic(method.getModifiers()));
    }

    @SuppressWarnings("unchecked")
    private static <R> R invokeMethod(@NonNull Class<?> clazz, @Nullable Object target, @NonNull String methodName, Object[] parameters, Method[] methods,
                                      Predicate<Method> methodPredicate) {
        Optional<Method> first = Arrays.stream(methods)
                .filter(method -> methodName.equals(method.getName()))
                .filter(methodPredicate)
                .filter(method -> matchingMethod(method.getParameterTypes(), parameters))
                .findFirst();

        var method = first.orElseThrow(() -> new UnsupportedOperationException("Can't find method %s on class %s".formatted(methodName, clazz.getName())));
        try {
            return (R) method.invoke(target, parameters);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to invoke method %s on object %s".formatted(methodName, clazz), e);
        }
    }

    private static boolean matchingMethod(Class<?>[] methodParameterTypes, Object... parameters) {
        if (methodParameterTypes.length != parameters.length) {
            return false;
        }
        boolean allMatch = true;
        for (int i = 0; i < parameters.length; i++) {
            // What about nulls?
            allMatch = allMatch && (methodParameterTypes[i].isAssignableFrom(parameters[i].getClass())
                    || (methodParameterTypes[i].isPrimitive() && primitiveToBox.get(methodParameterTypes[i]).isAssignableFrom(parameters[i].getClass())));
        }
        return allMatch;
    }
}
