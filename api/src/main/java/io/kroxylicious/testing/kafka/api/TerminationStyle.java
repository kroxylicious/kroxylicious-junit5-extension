/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.api;

public enum TerminationStyle {
    /**
     * Graceful termination style where components are requested to terminate and have the opportunity to close
     * resources (files, network connections etc.) that they are using before they exit.
     */
    GRACEFUL,

    /**
     * Abrupt termination style where components are killed immediately.
     */
    ABRUPT
}
