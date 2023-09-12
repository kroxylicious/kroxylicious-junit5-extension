/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum NodeRole {
    BROKER("broker"),
    CONTROLLER("controller");

    private final String configRole;

    private NodeRole(String configRole) {
        this.configRole = configRole;
    }

    /**
     * @return The role, as it can be configured in a Kafka {@code server.properties} file.
     */
    public static String forConfig(Collection<NodeRole> roles) {
        return roles.stream().map(x -> x.configRole).distinct().collect(Collectors.joining(","));
    }

    public static boolean isPureController(Set<NodeRole> roles) {
        return EnumSet.of(NodeRole.CONTROLLER).equals(roles);
    }

    public static boolean isCombinedNode(Set<NodeRole> roles) {
        return EnumSet.of(NodeRole.CONTROLLER, NodeRole.BROKER).equals(roles);
    }

    public static boolean isPureBroker(Set<NodeRole> roles) {
        return EnumSet.of(NodeRole.BROKER).equals(roles);
    }

    public static boolean hasBrokerRole(Set<NodeRole> roles) {
        return roles.contains(NodeRole.BROKER);
    }

    public static boolean hasControllerRole(Set<NodeRole> roles) {
        return roles.contains(NodeRole.CONTROLLER);
    }
}
