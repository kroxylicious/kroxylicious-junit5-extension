package io.kroxylicious.testing.kafka.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ScramInitialiserTest {

    @Test
    void shouldNotReferenceJUnitAssertions() throws IOException {
        try (var bytecode = ScramInitialiser.class.getResourceAsStream("ScramInitialiser.class")) {
            assertThat(bytecode).isNotNull();
            assertThat(new String(bytecode.readAllBytes(), StandardCharsets.ISO_8859_1))
                    .doesNotContain("org/junit/jupiter/api/Assertions");
        }
    }
}
