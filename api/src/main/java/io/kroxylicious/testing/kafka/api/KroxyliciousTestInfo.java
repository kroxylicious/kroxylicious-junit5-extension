/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.api;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.TestInfo;

public record KroxyliciousTestInfo(String displayName, Optional<Class<?>> testClass, Optional<Method> testMethod, Set<String> tags) implements TestInfo {
    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public Set<String> getTags() {
        return tags;
    }

    @Override
    public Optional<Class<?>> getTestClass() {
        return testClass;
    }

    @Override
    public Optional<Method> getTestMethod() {
        return testMethod;
    }
}
