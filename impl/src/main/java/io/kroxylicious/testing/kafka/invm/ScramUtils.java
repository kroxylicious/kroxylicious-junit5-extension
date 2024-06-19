/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.ScramCredential;

final class ScramUtils {

    private ScramUtils() {
        throw new IllegalStateException();
    }

    static ScramCredential asScramCredential(UserScramCredentialRecord uscr) {
        return new ScramCredential(uscr.salt(), uscr.storedKey(), uscr.serverKey(), uscr.iterations());
    }
}
