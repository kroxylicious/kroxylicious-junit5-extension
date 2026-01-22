/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.regex.Pattern;

import org.testcontainers.images.AbstractImagePullPolicy;
import org.testcontainers.images.ImageData;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import io.kroxylicious.testing.kafka.common.Version;

class FloatingTagPullPolicy extends AbstractImagePullPolicy implements ImagePullPolicy {
    @SuppressWarnings("java:S5852")
    private static final Pattern MAJOR_MINOR_PATCH = Pattern.compile("\\w*-?\\w*-?(\\d+)(\\.\\d+)?([.]\\d+)?");

    private final ImagePullPolicy defaultPullPolicy;
    private final Duration volatileImagePeriod;

    public static final ImagePullPolicy KAFKA_PULL_POLICY = new FloatingTagPullPolicy();

    public FloatingTagPullPolicy() {
        this(PullPolicy.defaultPolicy());
    }

    FloatingTagPullPolicy(ImagePullPolicy defaultPullPolicy) {
        this.defaultPullPolicy = defaultPullPolicy;
        volatileImagePeriod = Duration.ofDays(7);
    }

    @Override
    protected boolean shouldPullCached(DockerImageName imageName, ImageData localImageData) {
        final String versionTag = imageName.getVersionPart();
        if (Version.LATEST_RELEASE.equalsIgnoreCase(versionTag)) {
            return true;
        }
        else if (MAJOR_MINOR_PATCH.matcher(versionTag).matches()) {
            if (Instant.now().minus(volatileImagePeriod).isAfter(localImageData.getCreatedAt())) {
                return defaultPullPolicy.shouldPull(imageName);
            }
            else {
                return true;
            }
        }
        return false;
    }
}
