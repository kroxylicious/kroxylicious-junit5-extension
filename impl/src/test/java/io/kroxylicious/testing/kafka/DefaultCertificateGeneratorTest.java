/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.naming.InvalidNameException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultCertificateGeneratorTest {

    private static final Pattern CERT_PATTERN = Pattern.compile(
            "-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+" // Header
                    + "([a-z0-9+/=\\r\\n]+)"                     // Base64 text
                    + "-+END\\s+.*CERTIFICATE[^-]*-+",           // Footer
            CASE_INSENSITIVE);

    private static Stream<Arguments> generatesKeyStore() {
        return Stream.of(Arguments.of("PKCS12", "PKCS12"),
                Arguments.of("JKS", "JKS"),
                Arguments.of(null, KeyStore.getDefaultType().toUpperCase(Locale.ROOT)));
    }

    @ParameterizedTest
    @MethodSource
    public void generatesKeyStore(String storeType, String expectedStoreType) throws Exception {
        var alias = "test";
        var distinguishedName = "O=kroxylicious.io";
        var certGenerator = DefaultCertificateGenerator.builder()
                .storeType(storeType)
                .alias(alias)
                .distinguishedName(distinguishedName)
                .build();

        assertThat(certGenerator.getKeyStore()).isNotNull();
        var ks = loadKeyStore(certGenerator.getKeyStore(), certGenerator.getStorePassword(), certGenerator.getStoreType());
        assertThat(ks.size()).isEqualTo(1);
        assertThat(ks.getType()).isEqualTo(expectedStoreType);

        var cert = ks.getCertificate(alias);
        assertThat(cert).isNotNull();
        assertThat(((X509Certificate) cert).getSubjectX500Principal().getName()).isEqualTo(distinguishedName);

        var key = ks.getKey(alias, certGenerator.getKeyPassword().toCharArray());
        assertThat(key).isNotNull();
    }

    @Test
    public void detectsBadDistinguishedName() throws Exception {
        var alias = "test";
        var builder = DefaultCertificateGenerator.builder()
                .alias(alias)
                .distinguishedName("not a dn");

        assertThrows(InvalidNameException.class, () -> builder.build());
    }

    @Test
    public void generatesTrustStoreContainingExpectedCertificate() throws Exception {
        var alias = "test";
        var certGenerator = DefaultCertificateGenerator.builder()
                .alias(alias)
                .distinguishedName("O=kroxylicious.io")
                .build();

        assertThat(certGenerator.getKeyStore()).isNotNull();
        var ks = loadKeyStore(certGenerator.getKeyStore(), certGenerator.getStorePassword(), certGenerator.getStoreType());
        assertThat(ks).isNotNull();

        var certFromKeyStore = ks.getCertificate(alias);

        var ts = loadKeyStore(certGenerator.getTrustStore(), certGenerator.getStorePassword(), certGenerator.getStoreType());
        assertThat(ts).isNotNull();
        assertThat(ts.getType()).isEqualTo(ks.getType());
        assertThat(ts.size()).isEqualTo(1);

        var certFromTrustStore = ks.getCertificate(alias);
        assertThat(certFromTrustStore).isEqualTo(certFromKeyStore);
    }

    @Test
    public void generatesPemTrust() throws Exception {
        var alias = "test";
        var certGenerator = DefaultCertificateGenerator.builder()
                .alias(alias)
                .distinguishedName("O=kroxylicious.io")
                .build();

        assertThat(certGenerator.getKeyStore()).isNotNull();
        var ks = loadKeyStore(certGenerator.getKeyStore(), certGenerator.getStorePassword(), certGenerator.getStoreType());
        assertThat(ks).isNotNull();

        var certFromKeyStore = ks.getCertificate(alias);

        var chain = readCertificateChain(certGenerator.getTrustedCertficate());
        assertThat(chain).hasSize(1);

        var cert = chain.get(0);
        assertThat(cert).isEqualTo(certFromKeyStore);
    }



    private KeyStore loadKeyStore(File store, String storePassword, String storeType) throws Exception {
        var ks = KeyStore.getInstance(storeType);
        ks.load(new FileInputStream(store), storePassword.toCharArray());
        return ks;
    }

    public static List<X509Certificate> readCertificateChain(File certificateChainFile) throws IOException, GeneralSecurityException {
        var contents = Files.readString(certificateChainFile.toPath(), US_ASCII);
        var matcher = CERT_PATTERN.matcher(contents);
        var certificateFactory = CertificateFactory.getInstance("X.509");
        var certificates = new ArrayList<X509Certificate>();

        int start = 0;
        while (matcher.find(start)) {
            byte[] buffer = getMimeDecoder().decode(matcher.group(1).getBytes(US_ASCII));
            certificates.add((X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(buffer)));
            start = matcher.end();
        }

        return certificates;
    }


}