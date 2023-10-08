/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

public enum Role {
    BROKER("broker"),
    CONTROLLER("controller");

    private final String configString;

    Role(String configString) {
        this.configString = configString;
    }

    public String getConfigString() {
        return configString;
    }
}
