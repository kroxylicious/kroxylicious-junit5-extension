/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

public interface KeystoreManager {

    /**
     * Creates a CertificateBuilder with the appropriate default values for kroxylicious test usage.
     * @return The partially populated certificate builder.
     */
    CertificateBuilder newCertificateBuilder();

    /**
     * Builds and adds provided certificate builder as a self-signed certificate
     * @param certificateBuilder the builder configuring the certificate
     * @return the self-signed certificate.
     */
    X509Bundle createSelfSignedCertificate(CertificateBuilder certificateBuilder) throws Exception;

    /**
     * Optional we don't need this today!
     * Builds and adds provided certificate builder as a certificate signed by the provided issuer.
     * @param certificateBuilder the builder configuring the certificate
     * @return the signed certificate.
     */
    X509Bundle createSignedCertificate(X509Bundle issuer, CertificateBuilder certificateBuilder) throws Exception;

    /**
     * Formats the provided fields into a RFC5280 compliant form
     * @param email the email
     * @param organizationUnit the organization unit
     * @param organization the organization
     * @param city the city
     * @param state the state
     * @param country the country
     */
    String buildDistinguishedName(String email, String domain, String organizationUnit, String organization, String city, String state, String country);
}
