/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Signals the wish for a topic to be created and injected into a test as either test parameter
 * or field.
 * {@see KafkaClusterExtension}
 */
public interface Topic {
    @NonNull
    String name();
}
