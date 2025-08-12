/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.awaitility.Awaitility;

import io.kroxylicious.testing.kafka.internal.AdminSource;

import static org.junit.Assert.assertTrue;

public class ScramInitialiser {

    private ScramInitialiser() {
    }

    public static void initialiseScramUsers(AdminSource adminSource, KafkaClusterConfig clusterConfig) {
        try {
            if (clusterConfig.isSaslScram() && !clusterConfig.getUsers().isEmpty()) {
                ScramMechanism mechanism = clusterConfig.getScramMechanism()
                        .orElseThrow(() -> new RuntimeException("config is SASL scram, but scram mechanism is empty"));
                try (Admin admin = adminSource.createAdmin()) {
                    admin.alterUserScramCredentials(clusterConfig.getUsers().entrySet().stream()
                            .map(userEntry -> new UserScramCredentialUpsertion(userEntry.getKey(), new ScramCredentialInfo(mechanism, 4096), userEntry.getValue()))
                            .collect(Collectors.toList()))
                            .all().get(5, TimeUnit.SECONDS);
                    List<String> users = clusterConfig.getUsers().keySet().stream().toList();
                    // there is a delay in availability, if we don't pause here then some tests that immediately try to produce records fail to authenticate
                    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                        Map<String, UserScramCredentialsDescription> scramDescription = admin.describeUserScramCredentials(users).all()
                                .get(5, TimeUnit.SECONDS);
                        assertTrue(users.stream().allMatch(scramDescription::containsKey));
                    });
                }
            }
        }
        catch (Exception e) {
            throw new ScramInitializationException("Failed to initialise scram users", e);
        }
    }
}
