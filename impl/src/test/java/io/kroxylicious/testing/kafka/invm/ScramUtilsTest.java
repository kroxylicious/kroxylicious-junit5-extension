/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScramUtilsTest {
    @Test
    void asScramCredential() {
        int iterations = 4096;
        byte[] salt = "salt".getBytes(StandardCharsets.UTF_8);
        byte[] server = "key".getBytes(StandardCharsets.UTF_8);
        var uscr = new UserScramCredentialRecord()
                .setIterations(iterations)
                .setSalt(salt)
                .setServerKey(server);

        var sc = ScramUtils.asScramCredential(uscr);
        assertThat(sc).extracting(ScramCredential::iterations).isEqualTo(iterations);
        assertThat(sc).extracting(ScramCredential::salt).isEqualTo(salt);
        assertThat(sc).extracting(ScramCredential::serverKey).isEqualTo(server);
    }

    @Test
    void kafkaScramArgumentsForSingleUser() {
        var result = ScramUtils.toKafkaScramArguments("SCRAM-SHA-256", Map.of("user", "pwd"));
        assertThat(result).containsExactly("SCRAM-SHA-256=[name=user,password=pwd]");
    }

    @Test
    void unknownMechanismRejected() {
        var users = Map.<String, String> of();
        assertThatThrownBy(() -> ScramUtils.toKafkaScramArguments("PLAIN", users))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SCRAM-SHA-256=[name=alice,password=pwd]",
            "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]" })
    void generateCredentialsRecordsForSingleUser(String addScram) {
        var result = ScramUtils.getUserScramCredentialRecords(List.of(addScram));
        assertThat(result)
                .singleElement()
                .extracting(UserScramCredentialRecord::name)
                .isEqualTo("alice");
    }

    //
    @ParameterizedTest
    @ValueSource(strings = { "BAD=[name=user,password=pwd]",
            "SCRAM-SHA-256=malformed",
            "SCRAM-SHA-256malformed",
            "SCRAM-SHA-256=[malformed",
            "SCRAM-SHA-256=[missing-name=foo",
            "SCRAM-SHA-256=[name=user,password=pwd,unknown=component]"
    })
    void detectsInvalidScramArguments(String addScram) {
        var list = List.of(addScram);
        assertThatThrownBy(() -> ScramUtils.getUserScramCredentialRecords(list))
                .hasCauseInstanceOf(ScramUtils.FormatterException.class);
    }
}
