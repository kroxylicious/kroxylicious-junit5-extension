/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

/**
 * Used to configure and manage test certificates using the JDK's keytool.
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Requires ability to write test key material to file-system.")
public class KeytoolCertificateGenerator {
    private static final String PKCS12_KEYSTORE_TYPE = "PKCS12";
    private String password;
    private final Path certFilePath;
    private final Path keyStoreFilePath;
    private final Path trustStoreFilePath;
    private final System.Logger log = System.getLogger(KeytoolCertificateGenerator.class.getName());

    /**
     * Instantiates a new Keytool certificate generator.
     *
     * @throws IOException the io exception
     */
    public KeytoolCertificateGenerator() throws IOException {
        this(null, null);
    }

    /**
     * Instantiates a new Keytool certificate generator.
     *
     * @param certFilePath the cert file path
     * @param trustStorePath the trust store path
     * @throws IOException the io exception
     */
    public KeytoolCertificateGenerator(String certFilePath, String trustStorePath) throws IOException {
        Path certsDirectory = Files.createTempDirectory("kproxy");
        this.certFilePath = Path.of(certsDirectory.toAbsolutePath() + "/cert-file");
        this.keyStoreFilePath = (certFilePath != null) ? Path.of(certFilePath) : Paths.get(certsDirectory.toAbsolutePath().toString(), "kafka.keystore.jks");
        this.trustStoreFilePath = (trustStorePath != null) ? Path.of(trustStorePath) : Paths.get(certsDirectory.toAbsolutePath().toString(), "kafka.truststore.jks");

        certsDirectory.toFile().deleteOnExit();
        if (certFilePath == null) {
            this.keyStoreFilePath.toFile().deleteOnExit();
        }
        if (trustStorePath == null) {
            this.trustStoreFilePath.toFile().deleteOnExit();
        }
        this.certFilePath.toFile().deleteOnExit();
    }

    /**
     * Gets cert file path.
     *
     * @return the absolute path to the certificate file
     */
    public String getCertFilePath() {
        return certFilePath.toAbsolutePath().toString();
    }

    /**
     * Gets key store location.
     *
     * @return the absolute path to the key store
     */
    public String getKeyStoreLocation() {
        return keyStoreFilePath.toAbsolutePath().toString();
    }

    /**
     * Gets trust store location.
     *
     * @return the absolute path to the trust store
     */
    public String getTrustStoreLocation() {
        return trustStoreFilePath.toAbsolutePath().toString();
    }

    /**
     * Gets password.
     *
     * @return the password
     */
    public String getPassword() {
        if (password == null) {
            password = UUID.randomUUID().toString().replace("-", "");
        }
        return password;
    }

    /**
     * Can generate wildcard san.
     *
     * @return true if java version is greater or equal to 17, false otherwise
     */
    public boolean canGenerateWildcardSAN() {
        return Runtime.version().feature() >= 17;
    }

    /**
     * Generate trust store.
     *
     * @param certFilePath the cert file path
     * @param alias the alias
     * @throws GeneralSecurityException the general security exception
     * @throws IOException the io exception
     */
    public void generateTrustStore(String certFilePath, String alias) throws GeneralSecurityException, IOException {
        this.generateTrustStore(certFilePath, alias, getTrustStoreLocation());
    }

    /**
     * Generate trust store.
     *
     * @param certFilePath the cert file path
     * @param alias the alias
     * @param trustStoreFilePath the trust store file path
     * @throws GeneralSecurityException the general security exception
     * @throws IOException the io exception
     */
    public void generateTrustStore(String certFilePath, String alias, String trustStoreFilePath)
            throws GeneralSecurityException, IOException {
        // keytool -import -trustcacerts -keystore truststore.jks -storepass password -noprompt -alias localhost -file cert.crt
        if (Path.of(trustStoreFilePath).toFile().exists()) {
            var keyStore = KeyStore.getInstance(new File(trustStoreFilePath), getPassword().toCharArray());
            if (keyStore.containsAlias(alias)) {
                keyStore.deleteEntry(alias);
                keyStore.store(new FileOutputStream(trustStoreFilePath), getPassword().toCharArray());
            }
        }

        final List<String> commandParameters = new ArrayList<>(List.of("keytool", "-import", "-trustcacerts"));
        commandParameters.addAll(List.of("-keystore", trustStoreFilePath));
        commandParameters.addAll(List.of("-storepass", getPassword()));
        commandParameters.add("-noprompt");
        commandParameters.addAll(List.of("-alias", alias));
        commandParameters.addAll(List.of("-file", certFilePath));
        runCommand(commandParameters);
    }

    /**
     * Generate self-signed certificate entry.
     *
     * @param email the email
     * @param domain the domain
     * @param organizationUnit the organization unit
     * @param organization the organization
     * @param city the city
     * @param state the state
     * @param country the country
     * @throws GeneralSecurityException the general security exception
     * @throws IOException the io exception
     */
    public void generateSelfSignedCertificateEntry(String email, String domain, String organizationUnit,
                                                   String organization, String city, String state,
                                                   String country)
            throws GeneralSecurityException, IOException {

        if (keyStoreFilePath.toFile().exists()) {
            var keyStore = KeyStore.getInstance(keyStoreFilePath.toFile(), getPassword().toCharArray());
            keyStore.load(new FileInputStream(keyStoreFilePath.toFile()), getPassword().toCharArray());

            if (keyStore.containsAlias(domain)) {
                keyStore.deleteEntry(domain);
                keyStore.store(new FileOutputStream(keyStoreFilePath.toFile()), getPassword().toCharArray());
            }
        }

        final List<String> commandParameters = new ArrayList<>(List.of("keytool", "-genkey"));
        commandParameters.addAll(List.of("-alias", domain));
        commandParameters.addAll(List.of("-keyalg", "RSA"));
        commandParameters.addAll(List.of("-keysize", "2048"));
        commandParameters.addAll(List.of("-sigalg", "SHA256withRSA"));
        commandParameters.addAll(List.of("-storetype", PKCS12_KEYSTORE_TYPE));
        commandParameters.addAll(List.of("-keystore", getKeyStoreLocation()));
        commandParameters.addAll(List.of("-storepass", getPassword()));
        commandParameters.addAll(List.of("-keypass", getPassword()));
        commandParameters.addAll(
                List.of("-dname", getDomainName(email, domain, organizationUnit, organization, city, state, country)));
        commandParameters.addAll(List.of("-validity", "365"));
        if (canGenerateWildcardSAN() && !isWildcardDomain(domain)) {
            commandParameters.addAll(getSAN(domain));
        }
        runCommand(commandParameters);

        createCrtFileToImport(domain);
    }

    private void createCrtFileToImport(String alias) throws IOException {
        // keytool -export -keystore examplestore -alias signFiles -file Example.cer
        final List<String> commandParameters = new ArrayList<>(List.of("keytool", "-export", "-rfc"));
        commandParameters.addAll(List.of("-keystore", getKeyStoreLocation()));
        commandParameters.addAll(List.of("-storepass", getPassword()));
        commandParameters.addAll(List.of("-storetype", getKeyStoreType()));
        commandParameters.addAll(List.of("-alias", alias));
        commandParameters.addAll(List.of("-file", certFilePath.toAbsolutePath().toString()));
        runCommand(commandParameters);
    }

    private void runCommand(List<String> commandParameters) throws IOException {
        ProcessBuilder keytool = new ProcessBuilder().command(commandParameters);

        final Process process = keytool.start();
        try {
            process.waitFor();
        }
        catch (InterruptedException e) {
            throw new IOException("Keytool execution error");
        }

        log.log(DEBUG, "Generating certificate using `keytool` using command: {0}, parameters: {1}",
                process.info(), commandParameters);

        if (process.exitValue() > 0) {
            final String processError = (new BufferedReader(new InputStreamReader(process.getErrorStream()))).lines()
                    .collect(Collectors.joining(" \\ "));
            final String processOutput = (new BufferedReader(new InputStreamReader(process.getInputStream()))).lines()
                    .collect(Collectors.joining(" \\ "));
            log.log(WARNING, "Error generating certificate, error output: {0}, normal output: {1}, " +
                    "commandline parameters: {2}",
                    processError, processOutput, commandParameters);
            throw new IOException(
                    "Keytool execution error: '" + processError + "', output: '" + processOutput + "'" + ", commandline parameters: " + commandParameters);
        }
    }

    private boolean isWildcardDomain(String domain) {
        return domain.startsWith("*.");
    }

    private String getDomainName(String email, String domain, String organizationUnit, String organization, String city,
                                 String state, String country) {
        // keytool doesn't allow for a domain with wildcard in SAN extension, so it has to go directly to CN
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state +
                ", C=" + country + ", EMAILADDRESS=" + email;
    }

    private List<String> getSAN(String domain) {
        return List.of("-ext", "SAN=dns:" + domain);
    }

    public String getTrustStoreType() {
        return PKCS12_KEYSTORE_TYPE;
    }

    public String getKeyStoreType() {
        return PKCS12_KEYSTORE_TYPE;
    }
}
