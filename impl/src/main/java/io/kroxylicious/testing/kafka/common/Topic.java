/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface Topic {
    @NonNull
    String name();
}
