/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

public enum NamingStrategy {
    /**
     * name is composed of a random lowercase adjective, an underscore and random lowercase noun
     */
    RANDOM_ADJECTIVE_UNDERSCORE_NOUN,
    /**
     * name is composed of a random lowercase adjective, an underscore and random lowercase noun
     */
    RANDOM_ADJECTIVE_HYPHEN_NOUN
}
