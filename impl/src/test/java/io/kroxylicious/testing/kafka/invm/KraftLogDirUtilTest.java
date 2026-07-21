/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import kafka.server.KafkaConfig;

import static org.assertj.core.api.Assertions.assertThat;

class KraftLogDirUtilTest {

    private Path logDir;

    @BeforeEach
    void setUp() throws Exception {
        logDir = Files.createTempDirectory("kraft-test");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (logDir != null && Files.exists(logDir)) {
            try (Stream<Path> walk = Files.walk(logDir)) {
                walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }

    @Test
    void formatsLogDirectoryWithoutScramArguments() {
        var config = createMinimalKraftConfig();
        var clusterId = UUID.randomUUID().toString();

        KraftLogDirUtil.prepareLogDirsForKraft(clusterId, config);

        assertThat(logDir.resolve("meta.properties")).exists();
    }

    @Test
    void formatsLogDirectoryWithScramArguments() throws Exception {
        var config = createMinimalKraftConfig();
        var clusterId = UUID.randomUUID().toString();
        var scramArguments = List.of("SCRAM-SHA-256=[name=alice,password=alice-secret]");

        KraftLogDirUtil.prepareLogDirsForKraft(clusterId, config, scramArguments);

        assertThat(logDir.resolve("meta.properties")).exists();
        BootstrapMetadata metadata = new BootstrapDirectory(logDir.toString()).read();
        assertThat(metadata.records())
                .extracting(r -> r.message())
                .hasAtLeastOneElementOfType(UserScramCredentialRecord.class);
    }

    @Test
    void formatsLogDirectoryWithMultipleScramUsers() throws Exception {
        var config = createMinimalKraftConfig();
        var clusterId = UUID.randomUUID().toString();
        var scramArguments = List.of(
                "SCRAM-SHA-512=[name=alice,password=alice-secret]",
                "SCRAM-SHA-512=[name=bob,password=bob-secret]");

        KraftLogDirUtil.prepareLogDirsForKraft(clusterId, config, scramArguments);

        BootstrapMetadata metadata = new BootstrapDirectory(logDir.toString()).read();
        var scramRecords = metadata.records().stream()
                .map(r -> r.message())
                .filter(UserScramCredentialRecord.class::isInstance)
                .map(UserScramCredentialRecord.class::cast)
                .toList();
        assertThat(scramRecords)
                .extracting(UserScramCredentialRecord::name)
                .containsExactlyInAnyOrder("alice", "bob");
    }

    @Test
    void emptyScramArgumentsProducesNoScramRecords() throws Exception {
        var config = createMinimalKraftConfig();
        var clusterId = UUID.randomUUID().toString();

        KraftLogDirUtil.prepareLogDirsForKraft(clusterId, config, List.of());

        BootstrapMetadata metadata = new BootstrapDirectory(logDir.toString()).read();
        assertThat(metadata.records())
                .extracting(r -> r.message())
                .noneMatch(UserScramCredentialRecord.class::isInstance);
    }

    private KafkaConfig createMinimalKraftConfig() {
        Properties props = new Properties();
        props.setProperty("node.id", "0");
        props.setProperty("process.roles", "broker,controller");
        props.setProperty("controller.listener.names", "CONTROLLER");
        props.setProperty("controller.quorum.voters", "0@localhost:9093");
        props.setProperty("listeners", "CONTROLLER://localhost:9093,INTERNAL://localhost:9092");
        props.setProperty("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT");
        props.setProperty("inter.broker.listener.name", "INTERNAL");
        props.setProperty("advertised.listeners", "INTERNAL://localhost:9092");
        props.setProperty("log.dir", logDir.toAbsolutePath().toString());
        return new KafkaConfig(props);
    }
}
