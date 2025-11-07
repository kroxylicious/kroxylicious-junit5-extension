/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

public class KeystoreManager {
    private final ConcurrentMap<Path, String> passwords = new ConcurrentHashMap<>();

    /**
     * Creates a CertificateBuilder with the appropriate default values for kroxylicious test usage.
     * @param distinguishedName the distinguished name. See {@link KeystoreManager#buildDistinguishedName(String, String, String, String, String, String, String)}
     *                          for generating a distinguished name
     * @return  The partially populated certificate builder.
     */
    public CertificateBuilder newCertificateBuilder(String distinguishedName) {
        return new CertificateBuilder()
                .rsa2048()
                .subject(distinguishedName);
    }

    /**
     * Builds and adds provided certificate builder as a self-signed certificate
     * @param certificateBuilder the builder configuring the certificate
     * @return the self-signed certificate.
     */
    public X509Bundle createSelfSignedCertificate(CertificateBuilder certificateBuilder) throws Exception {
        return certificateBuilder.copy()
                .setIsCertificateAuthority(true)
                .buildSelfSigned();
    }

    /**
     * Optional we don't need this today!
     * Builds and adds provided certificate builder as a certificate signed by the provided issuer.
     * @param certificateBuilder the builder configuring the certificate
     * @return the signed certificate.
     */
    public X509Bundle createSignedCertificate(X509Bundle issuer, CertificateBuilder certificateBuilder) throws Exception {
        return certificateBuilder.copy()
                .buildIssuedBy(issuer);
    }

    /**
     * Formats the provided fields into a RFC5280 compliant form
     * @param email the email
     * @param domain the domain
     * @param organizationUnit the organization unit
     * @param organization the organization
     * @param city the city
     * @param state the state
     * @param country the country
     * @return the distinguished name
     */
    public String buildDistinguishedName(String email, String domain, String organizationUnit, String organization, String city, String state, String country) {
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state + ", C=" + country + ", EMAILADDRESS=" + email;
    }

    /**
     * Gets password.
     *
     * @return  the password
     */
    public String getPassword(Path keystorePath) {
        // hyphen is removed to make our debugging easier: copy-pasting with hyphens not always copy the whole password
        return this.passwords.computeIfAbsent(keystorePath, path -> UUID.randomUUID().toString().replace("-", ""));
    }

    /**
     * Generate a keystore at returned path. It contains both certificates, subject's (key certs) and issuer's (trust CA certs)
     * Use {@link KeystoreManager#getPassword(Path)} for getting the keystore password.
     *
     * @param bundle the bundle
     * @return  the path of the generated certificate file
     * @throws KeyStoreException the key store exception
     * @throws IOException the io exception
     * @throws CertificateException the certificate exception
     * @throws NoSuchAlgorithmException the no such algorithm exception
     */
    public Path generateCertificateFile(X509Bundle bundle) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
        Path certsDirectory = Files.createTempDirectory("kroxylicious", attr);
        Path keyStoreFilePath = Paths.get(certsDirectory.toAbsolutePath().toString(), "keystore.jks");
        KeyStore keyStore = bundle.toKeyStore(getPassword(keyStoreFilePath).toCharArray());
        try (FileOutputStream stream = new FileOutputStream(keyStoreFilePath.toFile())) {
            keyStore.store(stream, getPassword(keyStoreFilePath).toCharArray());
        }
        keyStoreFilePath.toFile().deleteOnExit();
        certsDirectory.toFile().deleteOnExit();

        return keyStoreFilePath;
    }
}
