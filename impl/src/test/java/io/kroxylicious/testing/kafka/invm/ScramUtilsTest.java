/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
