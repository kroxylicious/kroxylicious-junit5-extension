/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka;

import io.kroxylicious.testing.kafka.common.CertificateGenerator;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

import javax.naming.ldap.LdapName;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

@Getter
@ToString
public class DefaultCertificateGenerator implements CertificateGenerator {

    private final System.Logger log = System.getLogger(DefaultCertificateGenerator.class.getName());

    private final String storeType;

    private final String alias;

    private final String distinguishedName;

    @Singular
    private final List<String> sans;

    private final Path keystorePath;
    private final Path trustStorePath;
    private final Path trustPemPath;
    private final LdapName dname;
    private final String storePassword;
    private final String keyPassword;

    @Builder(toBuilder = true)
    public DefaultCertificateGenerator(String storeType, String alias, String distinguishedName, List<String> sans) throws Exception {
        Objects.requireNonNull(alias);
        Objects.requireNonNull(distinguishedName);
        this.storeType = storeType == null ? KeyStore.getDefaultType().toUpperCase(Locale.ROOT) : storeType.toUpperCase(Locale.ROOT);
        this.alias = alias;
        this.distinguishedName = distinguishedName;
        this.sans = sans == null ? List.of() : new ArrayList<>(sans);


        this.storePassword = UUID.randomUUID().toString().replace("-", "");
        // PKCS12 keystore format doesn't allow a different password
        this.keyPassword = Objects.equals(this.storeType, "PKCS12") ? storePassword : UUID.randomUUID().toString().replace("-", "");

        this.dname = new LdapName(distinguishedName);
        var keyMaterial = Files.createTempDirectory(alias);

        var ext = Objects.equals("PKCS12", storeType) ? "p12" : this.storeType.toLowerCase(Locale.ROOT);
        this.keystorePath = keyMaterial.resolve("keyStore" + "." + ext);
        this.trustStorePath = keyMaterial.resolve("trustStore" + "." + ext);
        this.trustPemPath = keyMaterial.resolve("trust" + ".pem");

        var ks = generateSelfSignedKeyStore();
        var serverCert = ks.getCertificate(this.alias);
        generateTrustStore(serverCert);
        writePemToTemporaryFile(List.of(serverCert), this.trustPemPath);
    }

    @Override
    public File getKeyStore() {
        return keystorePath.toFile();
    }

    @Override
    public File getTrustStore() {
        return trustStorePath.toFile();
    }

    private KeyStore generateSelfSignedKeyStore() throws Exception  {

        final List<String> commandParameters = new ArrayList<>(List.of("keytool", "-genkey"));
        commandParameters.addAll(List.of("-alias", getAlias()));
        commandParameters.addAll(List.of("-keyalg", "RSA"));
        commandParameters.addAll(List.of("-keysize", "2048"));
        commandParameters.addAll(List.of("-sigalg", "SHA256withRSA"));
        commandParameters.addAll(List.of("-storetype", getStoreType()));
        commandParameters.addAll(List.of("-keystore", this.keystorePath.toString()));
        commandParameters.addAll(List.of("-storepass", this.storePassword));
        commandParameters.addAll(List.of("-keypass", this.keyPassword));
        commandParameters.addAll(List.of("-dname", this.dname.toString()));
        commandParameters.addAll(List.of("-validity", "365"));
        if (!sans.isEmpty()) {
            commandParameters.addAll(List.of("-ext", sans.stream().collect(Collectors.joining(",", "SAN=", ""))));
        }
        runCommand(commandParameters);

        var storePassword = this.storePassword.toCharArray();
        var ks = KeyStore.getInstance(getStoreType());
        try (FileInputStream stream = new FileInputStream(this.keystorePath.toFile())) {
            ks.load(stream, storePassword);
        }

        return ks;
    }

    private void generateTrustStore(Certificate certificate) throws Exception {
        var storePassword = this.storePassword.toCharArray();

        var ts = KeyStore.getInstance(getStoreType());
        ts.load(null, null);

        ts.setCertificateEntry(alias, certificate);
        try (var fos = new FileOutputStream(this.trustStorePath.toFile())) {
            ts.store(fos, storePassword);
        }
    }

    @Override
    public File getTrustedCertficate() {
        return this.trustPemPath.toFile();
    }

    private void runCommand(List<String> commandParameters) throws IOException {
        var keytool = new ProcessBuilder().command(commandParameters);

        final Process process = keytool.start();
        try {
            process.waitFor();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Keytool execution error", e);
        }

        log.log(DEBUG, "Generating certificate using `keytool` using command: {0}, parameters: {1}",
                process.info(), commandParameters);

        if (process.exitValue() != 0) {
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

    private void writePemToTemporaryFile(List<Certificate> certificates, Path trustPemPath1) throws Exception {
        var mimeLineEnding = new byte[]{ '\r', '\n' };

        try (var out = new FileOutputStream(trustPemPath1.toFile())) {
            certificates.forEach(c -> {
                var encoder = Base64.getMimeEncoder();
                try {
                    out.write("-----BEGIN CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                    out.write(mimeLineEnding);
                    out.write(encoder.encode(c.getEncoded()));
                    out.write(mimeLineEnding);
                    out.write("-----END CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                    out.write(mimeLineEnding);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                catch (CertificateEncodingException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
