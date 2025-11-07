/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.common;

import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class KeystoreTest {
    private static final int ASN_GENERAL_NAME_IP_ADDRESS = 7;
    private static final int ASN_GENERAL_NAME_DNS = 2;

    @Test
    void generateSelfSignedKeyStore() throws Exception {
        var keystore = new KeystoreManager();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US"));
        X509Bundle bundle = keystore.createSelfSignedCertificate(certificateBuilder);

        assertAll(() -> {
            assertThat(bundle.getCertificate()).isNotNull();
            assertThat(bundle.isSelfSigned()).isTrue();
            assertThat(bundle.isCertificateAuthority()).isTrue();
        });
    }

    @Test
    void generateSignedKeyStore() throws Exception {
        var keystore = new KeystoreManager();
        CertificateBuilder issuerCertificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("mail@kroxylicious.io", "localhost", "TestCA",
                "Issuer", null, null, "US"));

        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US"));

        X509Bundle issuer = keystore.createSelfSignedCertificate(issuerCertificateBuilder);
        X509Bundle signed = keystore.createSignedCertificate(issuer, certificateBuilder);

        assertAll(() -> {
            assertThat(signed.getCertificate()).isNotNull();
            assertThat(signed.getCertificate().getIssuerX500Principal()).isEqualTo(issuer.getCertificate().getSubjectX500Principal());
            assertThat(signed.isSelfSigned()).isFalse();
            assertThat(signed.isCertificateAuthority()).isFalse();
        });
    }

    @Test
    void generateKeyStoreWithIPDomain() throws Exception {
        String domainIP = "127.0.0.1";
        var keystore = new KeystoreManager();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US"));
        keystore.addSanNames(certificateBuilder, List.of(domainIP));
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
        var keystore = new KeystoreManager();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US"));//, List.of(domain));
        keystore.addSanNames(certificateBuilder, List.of(domain));
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
        var keystore = new KeystoreManager();
        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US"));//, List.of(domain, domainIP));
        keystore.addSanNames(certificateBuilder, List.of(domain, domainIP));

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

    @Test
    void generateKeyStoreFile() throws Exception {
        String domain = "localhost";
        var keystore = new KeystoreManager();
        CertificateBuilder issuerCertificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("mail@kroxylicious.io", domain, "TestCA",
                "Issuer", null, null, "US"));

        CertificateBuilder certificateBuilder = keystore.newCertificateBuilder(keystore.buildDistinguishedName("test@kroxylicious.io", domain, "Dev",
                "Kroxylicious.io", null, null, "US"));

        X509Bundle issuer = keystore.createSelfSignedCertificate(issuerCertificateBuilder);
        X509Bundle signed = keystore.createSignedCertificate(issuer, certificateBuilder);

        Path keyStoreFilePath = keystore.generateCertificateFile(signed);
        String password = keystore.getPassword(keyStoreFilePath);
        assertThat(keyStoreFilePath.toFile()).exists();
        var ks = KeyStore.getInstance(keyStoreFilePath.toFile(), password.toCharArray());
        var keyAliases = keyAliasList(ks);
        assertThat(keyAliases).hasSize(1);
        var keyAlias = keyAliases.get(0);

        var certAliases = certificateAliasList(ks);
        var certAlias = certAliases.get(0);
        assertAll(() -> {
            assertThat(certAliases).hasSize(1);
            assertThat(certAlias).isNotEqualTo(keyAlias);
        });

        assertAll(() -> {
            assertThat(ks.getCertificate(keyAlias)).isEqualTo(signed.getCertificate());
            assertThat(ks.getCertificate(certAlias)).isEqualTo(issuer.getCertificate());
            assertThat(ks.getType()).isEqualTo(signed.toKeyStore(password.toCharArray()).getType());
        });
    }

    @NonNull
    private List<String> aliasList(KeyStore ks) throws KeyStoreException {
        List<String> aliases = new ArrayList<>();
        ks.aliases().asIterator().forEachRemaining(aliases::add);
        return aliases;
    }

    @NonNull
    private List<String> keyAliasList(KeyStore ks) throws KeyStoreException {
        return aliasList(ks).stream().filter(a -> {
            try {
                return ks.isKeyEntry(a);
            }
            catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }

    @NonNull
    private List<String> certificateAliasList(KeyStore ks) throws KeyStoreException {
        return aliasList(ks).stream().filter(a -> {
            try {
                return ks.isCertificateEntry(a);
            }
            catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }
}
