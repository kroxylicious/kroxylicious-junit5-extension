/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.time.Instant;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import static java.time.temporal.ChronoUnit.DAYS;

public class Keystore implements KeystoreManager {

    @Override
    public CertificateBuilder newCertificateBuilder() {
        Instant now = Instant.now();
        return new CertificateBuilder()
                .notBefore(now.minus(1, DAYS))
                .notAfter(now.plus(1, DAYS))
                .rsa2048();
    }

    /**
     * New certificate builder.
     *
     * @param distinguishedName the distinguished name
     * @return  the certificate builder
     */
    public CertificateBuilder newCertificateBuilder(String distinguishedName) {
        return newCertificateBuilder().copy()
                .subject(distinguishedName);
    }

    @Override
    public X509Bundle createSelfSignedCertificate(CertificateBuilder certificateBuilder) throws Exception {
        return certificateBuilder.copy()
                .setIsCertificateAuthority(true)
                .buildSelfSigned();
    }

    @Override
    public X509Bundle createSignedCertificate(X509Bundle issuer, CertificateBuilder certificateBuilder) throws Exception {
        return certificateBuilder.copy()
                .buildIssuedBy(issuer);
    }

    @Override
    public String buildDistinguishedName(String email, String domain, String organizationUnit, String organization, String city, String state, String country) {
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state + ", C=" + country + ", EMAILADDRESS=" + email;
    }
}
