/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.common;

import java.security.cert.X509Certificate;
import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class KeystoreTest {
    private static final int ASN_GENERAL_NAME_IP_ADDRESS = 7;
    private static final int ASN_GENERAL_NAME_DNS = 2;

    @Test
    void generateSelfSignedKeyStore() throws Exception {
        var keystore = new Keystore();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder()
                .subject(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                        "Kroxylicious.io", null, null, "US"));
        X509Bundle bundle = keystore.createSelfSignedCertificate(certificateBuilder);

        assertAll(() -> {
            assertThat(bundle.getCertificate()).isNotNull();
            assertThat(bundle.isSelfSigned()).isTrue();
            assertThat(bundle.isCertificateAuthority()).isTrue();
        });
    }

    @Test
    void generateSelfSignedKeyStoreAddingDistinguishedName() throws Exception {
        var keystore = new Keystore();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost",
                "Dev", "Kroxylicious.io", null, null, "US"));
        X509Bundle bundle = keystore.createSelfSignedCertificate(certificateBuilder);

        assertAll(() -> {
            assertThat(bundle.getCertificate()).isNotNull();
            assertThat(bundle.isSelfSigned()).isTrue();
            assertThat(bundle.isCertificateAuthority()).isTrue();
        });
    }

    @Test
    void generateSignedKeyStore() throws Exception {
        var keystore = new Keystore();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder()
                .subject(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                        "Kroxylicious.io", null, null, "US"));
        X509Bundle issuer = keystore.createSelfSignedCertificate(certificateBuilder);
        X509Bundle signed = keystore.createSignedCertificate(issuer, certificateBuilder);

        assertAll(() -> {
            assertThat(signed.getCertificate()).isNotNull();
            assertThat(signed.getCertificate().getIssuerX500Principal()).isEqualTo(issuer.getCertificate().getSubjectX500Principal());
            assertThat(signed.isSelfSigned()).isTrue();
            assertThat(signed.isCertificateAuthority()).isFalse();
        });
    }

    @Test
    void generateKeyStoreWithIPDomain() throws Exception {
        String domainIP = "127.0.0.1";
        var keystore = new Keystore();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder()
                .subject(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                        "Kroxylicious.io", null, null, "US"))
                .addSanIpAddress(domainIP);
        X509Bundle bundle = keystore.createSelfSignedCertificate(certificateBuilder);

        List<?> expectedIp = List.of(KeystoreTest.ASN_GENERAL_NAME_IP_ADDRESS, domainIP);

        assertThat(bundle.getCertificate())
                .asInstanceOf(InstanceOfAssertFactories.type(X509Certificate.class))
                .satisfies(c -> {
                    var names = c.getSubjectAlternativeNames();
                    assertThat(names)
                            .singleElement()
                            .isEqualTo(expectedIp);
                });
    }

    @Test
    void generateKeyStoreWithDnsSAN() throws Exception {
        String domain = "localhost";
        var keystore = new Keystore();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder()
                .subject(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                        "Kroxylicious.io", null, null, "US"))
                .addSanDnsName(domain);
        X509Bundle bundle = keystore.createSelfSignedCertificate(certificateBuilder);

        List<?> expectedDns = List.of(KeystoreTest.ASN_GENERAL_NAME_DNS, domain);

        assertThat(bundle.getCertificate())
                .asInstanceOf(InstanceOfAssertFactories.type(X509Certificate.class))
                .satisfies(c -> {
                    var names = c.getSubjectAlternativeNames();
                    assertThat(names)
                            .singleElement()
                            .isEqualTo(expectedDns);
                });
    }

    @Test
    void generatesKeyStoreWithIPAndDnsSAN() throws Exception {
        String domain = "localhost";
        String domainIP = "127.0.0.1";
        var keystore = new Keystore();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder()
                .subject(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                        "Kroxylicious.io", null, null, "US"))
                .addSanIpAddress(domainIP)
                .addSanDnsName(domain);

        X509Bundle bundle = keystore.createSelfSignedCertificate(certificateBuilder);

        List<?> expectedDns = List.of(KeystoreTest.ASN_GENERAL_NAME_DNS, domain);
        List<?> expectedIp = List.of(KeystoreTest.ASN_GENERAL_NAME_IP_ADDRESS, domainIP);

        assertThat(bundle.getCertificate())
                .asInstanceOf(InstanceOfAssertFactories.type(X509Certificate.class))
                .satisfies(c -> {
                    var names = c.getSubjectAlternativeNames();
                    assertThat(names)
                            .anyMatch(x -> x.equals(expectedDns))
                            .anyMatch(x -> x.equals(expectedIp));
                });
    }
}
