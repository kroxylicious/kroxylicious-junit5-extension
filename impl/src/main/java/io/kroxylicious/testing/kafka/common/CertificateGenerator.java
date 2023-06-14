/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.File;

public interface CertificateGenerator {
    File getKeyStore();

    File getTrustStore();

    String getStoreType();

    String getStorePassword();

    String getKeyPassword();

    File getTrustedCertficate();
}
