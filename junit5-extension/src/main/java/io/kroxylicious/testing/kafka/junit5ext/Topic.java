/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.Optional;

import org.apache.kafka.common.Uuid;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Signals the wish for a topic to be created and injected into a test as either test parameter
 * or field.
 * {@see KafkaClusterExtension}
 */
public interface Topic {
    @NonNull
    String name();

    /**
     * @return the topic id of the created topic, will be non-empty for kafka versions >=2.8
     */
    @NonNull
    Optional<Uuid> topicId();
}
