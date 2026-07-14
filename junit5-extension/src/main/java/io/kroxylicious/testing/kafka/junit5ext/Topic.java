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
 * or field. To customize the naming of the topic, parameters and fields of this type should be
 * annotated with {@link TopicNameMethodSource}.
 * {@see KafkaClusterExtension}
 */
public interface Topic {
    /**
     * Returns the name of the created topic.
     *
     * @return the name of the created topic
     */
    @NonNull
    String name();

    /**
     * Returns the topic id assigned by the broker.
     *
     * @return the topic id of the created topic, will be non-empty for kafka versions &gt;=2.8
     */
    @NonNull
    Optional<Uuid> topicId();
}
