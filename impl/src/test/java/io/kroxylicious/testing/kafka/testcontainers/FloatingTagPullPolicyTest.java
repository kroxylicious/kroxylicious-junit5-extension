package io.kroxylicious.testing.kafka.testcontainers;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.images.ImageData;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.common.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class FloatingTagPullPolicyTest {

    private FloatingTagPullPolicy floatingTagPullPolicy;

    @Mock
    ImagePullPolicy defaultPullPolicy;

    @BeforeEach
    void setUp() {
        floatingTagPullPolicy = new FloatingTagPullPolicy(defaultPullPolicy);
    }

    @ParameterizedTest(name = "Should always pull Snapshot when cached image is: {0}")
    @MethodSource("relativeDates")
    void shouldAlwaysPullSnapshot(Instant imageCreatedAt) {
        // Given
        final ImageData imageData = ImageData.builder().createdAt(imageCreatedAt).build();

        // When
        final boolean actual = floatingTagPullPolicy.shouldPullCached(
                DockerImageName.parse("quay.io/kroxylicious").withTag(Version.LATEST_SNAPSHOT),
                imageData);

        // Then
        assertThat(actual).isTrue();
    }

    @ParameterizedTest(name = "Should pull release when image is: {0}")
    @MethodSource("relativeDates")
    void shouldAlwaysPullRelease(Instant imageCreatedAt) {
        // Given
        final ImageData imageData = ImageData.builder().createdAt(imageCreatedAt).build();

        // When
        final boolean actual = floatingTagPullPolicy.shouldPullCached(
                DockerImageName.parse("quay.io/kroxylicious").withTag(Version.LATEST_RELEASE),
                imageData);

        // Then
        assertThat(actual).isTrue();
    }

    @Test
    void shouldDelegateToDefaultPolicyReleasedVersionIfCacheIsMoreThan7DaysOld() {
        // Given
        final ImageData imageData = ImageData.builder().createdAt(Instant.now().truncatedTo(ChronoUnit.DAYS).minus(7, ChronoUnit.DAYS)).build();
        final DockerImageName taggedImage = DockerImageName.parse("quay.io/kroxylicious").withTag("latest-kafka-3.6.0");

        // When
        floatingTagPullPolicy.shouldPullCached(taggedImage, imageData);

        // Then
        verify(defaultPullPolicy).shouldPull(taggedImage);
    }

    @Test
    void shouldPullReleasedVersionIfCacheIsLessThan7DaysOld() {
        // Given
        final ImageData imageData = ImageData.builder().createdAt(Instant.now().truncatedTo(ChronoUnit.HALF_DAYS)).build();
        final DockerImageName taggedImage = DockerImageName.parse("quay.io/kroxylicious").withTag("latest-kafka-3.6.0");

        // When
        final boolean actual = floatingTagPullPolicy.shouldPullCached(
                taggedImage,
                imageData);

        // Then
        verifyNoInteractions(defaultPullPolicy);
        assertThat(actual).isTrue();
    }

    public static Stream<Arguments> relativeDates() {
        return Stream.of(
                arguments(named("millis old", Instant.now())),
                arguments(named("hours old", Instant.now().truncatedTo(ChronoUnit.HALF_DAYS))),
                arguments(named("24 hours old", Instant.now().truncatedTo(ChronoUnit.HOURS))),
                arguments(named("7 days old", Instant.now().truncatedTo(ChronoUnit.DAYS).minus(7, ChronoUnit.DAYS))),
                arguments(named("6 months old", Instant.now().minus(180, ChronoUnit.DAYS)))
        );
    }
}
