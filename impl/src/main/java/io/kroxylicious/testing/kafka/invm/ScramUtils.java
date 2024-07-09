/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import edu.umd.cs.findbugs.annotations.NonNull;
import kafka.tools.StorageTool;

final class ScramUtils {

    private ScramUtils() {
        throw new IllegalStateException();
    }

    static ScramCredential asScramCredential(UserScramCredentialRecord uscr) {
        return new ScramCredential(uscr.salt(), uscr.storedKey(), uscr.serverKey(), uscr.iterations());
    }

    @NonNull
    static List<UserScramCredentialRecord> getScramCredentialRecords(String saslMechanism, Map<String, String> users) {
        var scramMechanism = ScramMechanism.forMechanismName(saslMechanism);
        if (scramMechanism == null) {
            throw new IllegalArgumentException("unexpected scram mechanism " + saslMechanism);
        }
        return users
                .entrySet()
                .stream()
                .map(e -> StorageTool.getUserScramCredentialRecord(scramMechanism.mechanismName(), toKafkaScramCredentialsFormat(e.getKey(), e.getValue())))
                .toList();
    }

    @NonNull
    private static String toKafkaScramCredentialsFormat(String username, String password) {
        return "[name=%s,password=%s]".formatted(username, password);
    }
}
