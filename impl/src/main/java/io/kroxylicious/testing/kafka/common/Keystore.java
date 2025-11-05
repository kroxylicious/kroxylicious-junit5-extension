package io.kroxylicious.testing.kafka.common;

import java.time.Instant;
import java.util.UUID;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import static java.time.temporal.ChronoUnit.DAYS;

public class Keystore implements KeystoreManager {
    private String password;

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
        Instant now = Instant.now();
        return new CertificateBuilder()
                .notBefore(now.minus(1, DAYS))
                .notAfter(now.plus(1, DAYS))
                .subject(distinguishedName)
                .rsa2048();
    }

    @Override
    public X509Bundle createSelfSignedCertificate(CertificateBuilder certificateBuilder) {
        try {
            return certificateBuilder.copy()
                    .setIsCertificateAuthority(true)
                    .buildSelfSigned();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public X509Bundle createSignedCertificate(X509Bundle issuer, CertificateBuilder certificateBuilder) {
        try {
            return certificateBuilder.copy()
                    .buildIssuedBy(issuer);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String buildDistinguishedName(String email, String domain, String organizationUnit, String organization, String city, String state, String country) {
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state + ", C=" + country + ", EMAILADDRESS=" + email;
    }

    /**
     * Creates a password.
     *
     * @return the password
     */
    public String getPassword() {
        if (this.password == null) {
            this.password = UUID.randomUUID().toString().replace("-", "");
        }

        return this.password;
    }
}
