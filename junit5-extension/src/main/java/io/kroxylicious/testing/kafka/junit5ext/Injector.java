/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Type;

import org.junit.jupiter.api.extension.ExtensionContext;

@FunctionalInterface
interface Injector<T, X extends T> {
    X inject(String description,
             AnnotatedElement sourceElement,
             Class<X> type,
             Type genericType,
             ExtensionContext extensionContext);
}
