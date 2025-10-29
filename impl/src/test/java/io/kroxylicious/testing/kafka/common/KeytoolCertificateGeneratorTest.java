/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.common;

import java.io.File;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class KeytoolCertificateGeneratorTest {
    private static final int ASN_GENERAL_NAME_IP_ADDRESS = 7;

    @Test
    public void generatesKeyStore() throws Exception {
        var generator = new KeytoolCertificateGenerator();
        generator.generateSelfSignedCertificateEntry("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US");

        var keystore = generator.getKeyStoreLocation();
        assertThat(keystore).isNotEmpty();
        var keystoreFile = new File(keystore);
        assertThat(keystoreFile).exists();
        var password = generator.getPassword();

        var ks = KeyStore.getInstance(keystoreFile, password.toCharArray());
        var aliases = aliasList(ks);
        assertThat(aliases).hasSize(1);
        var alias = aliases.get(0);
        assertThat(ks.getCertificate(alias)).isNotNull();
        assertThat(ks.getKey(alias, password.toCharArray())).isNotNull();
        assertThat(ks.getType()).isEqualTo(generator.getKeyStoreType());
    }

    @Test
    public void generatesKeyStoreWithIPDomain() throws Exception {
        var generator = new KeytoolCertificateGenerator();
        generator.generateSelfSignedCertificateEntry("test@kroxylicious.io", "127.0.0.1", "Dev",
                "Kroxylicious.io", null, null, "US");

        var keystore = generator.getKeyStoreLocation();
        assertThat(keystore).isNotEmpty();
        var keystoreFile = new File(keystore);
        assertThat(keystoreFile).exists();
        var password = generator.getPassword();

        var ks = KeyStore.getInstance(keystoreFile, password.toCharArray());
        var aliases = aliasList(ks);
        var alias = aliases.get(0);
        assertThat(ks.getCertificate(alias))
                .asInstanceOf(InstanceOfAssertFactories.type(X509Certificate.class))
                .satisfies(c -> {
                    var names = c.getSubjectAlternativeNames();
                    assertThat(names)
                            .singleElement()
                            .asInstanceOf(InstanceOfAssertFactories.list(List.class))
                            .isEqualTo(List.of(KeytoolCertificateGeneratorTest.ASN_GENERAL_NAME_IP_ADDRESS, "127.0.0.1"));

                });
    }

    @Test
    public void generatesTrustStore() throws Exception {
        var generator = new KeytoolCertificateGenerator();
        generator.generateSelfSignedCertificateEntry("test@kroxylicious.io", "localhost", "Dev",
                "Kroxylicious.io", null, null, "US");

        var myAlias = "alias";
        // Weird API
        generator.generateTrustStore(generator.getCertFilePath(), myAlias);

        var trustStore = generator.getTrustStoreLocation();
        assertThat(trustStore).isNotEmpty();
        var trustStoreFile = new File(trustStore);
        assertThat(trustStoreFile).exists();
        var password = generator.getPassword();

        var ts = KeyStore.getInstance(trustStoreFile, password.toCharArray());
        var aliases = aliasList(ts);
        assertThat(aliases).hasSize(1);
        var alias = aliases.get(0);
        assertThat(alias).isEqualTo(myAlias);
        assertThat(ts.getCertificate(alias)).isNotNull();
        assertThat(ts.getType()).isEqualTo(generator.getTrustStoreType());
    }

    @NonNull
    private List<String> aliasList(KeyStore ks) throws KeyStoreException {
        List<String> aliases = new ArrayList<>();
        ks.aliases().asIterator().forEachRemaining(aliases::add);
        return aliases;
    }
}
