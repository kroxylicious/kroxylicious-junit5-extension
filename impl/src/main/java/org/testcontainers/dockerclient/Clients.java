/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.testcontainers.dockerclient;

import java.time.Duration;
import java.util.Collections;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.shaded.com.github.dockerjava.core.DefaultDockerClientConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.DockerClientImpl;
import org.testcontainers.shaded.com.github.dockerjava.core.RemoteApiVersion;
import org.testcontainers.utility.TestcontainersConfiguration;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.zerodep.TweakedDockerHttpClient;

/**
 * In this package to access AuthDelegatingDockerClientConfig and HeadersAddingDockerHttpClient
 */
public class Clients {

    public static DockerClient getClientForConf(TransportConfig transportConfig) {
        final DockerHttpClient dockerHttpClient;

        String transportType = TestcontainersConfiguration.getInstance().getTransportType();
        switch (transportType) {
            case "httpclient5":
                dockerHttpClient = new TweakedDockerHttpClient(
                        transportConfig.getDockerHost(),
                        transportConfig.getSslConfig(),
                        Integer.MAX_VALUE,
                        3,
                        null,
                        null,
                        Duration.ofSeconds(8L),
                        4,
                        Duration.ofMillis(100));
                break;
            default:
                throw new IllegalArgumentException("Unknown transport type '" + transportType + "'");
        }

        DefaultDockerClientConfig.Builder configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder();

        if (configBuilder.build().getApiVersion() == RemoteApiVersion.UNKNOWN_VERSION) {
            configBuilder.withApiVersion(RemoteApiVersion.VERSION_1_32);
        }
        return DockerClientImpl.getInstance(
                new AuthDelegatingDockerClientConfig(
                        configBuilder.withDockerHost(transportConfig.getDockerHost().toString()).build()),
                new HeadersAddingDockerHttpClient(
                        dockerHttpClient,
                        Collections.singletonMap("x-tc-sid", DockerClientFactory.SESSION_ID)));
    }
}
