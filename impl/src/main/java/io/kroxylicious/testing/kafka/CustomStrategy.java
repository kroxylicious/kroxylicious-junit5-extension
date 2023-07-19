/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka;

import java.util.Optional;

import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.dockerclient.TransportConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.DefaultDockerClientConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.DockerClientConfig;
import org.testcontainers.utility.TestcontainersConfiguration;

import com.github.dockerjava.api.DockerClient;

import lombok.Getter;

import static org.testcontainers.dockerclient.Clients.getClientForConf;

public class CustomStrategy extends DockerClientProviderStrategy {
    public static final int PRIORITY = 100;

    private final DockerClientConfig dockerClientConfig;

    @Getter
    private final boolean applicable;

    public CustomStrategy() {
        // use docker-java defaults if present, overridden if our own configuration is set
        this(DefaultDockerClientConfig.createDefaultConfigBuilder());
    }

    CustomStrategy(DefaultDockerClientConfig.Builder configBuilder) {
        Optional<String> dockerHost = getSetting("kroxy.docker.host");
        dockerHost.ifPresent(configBuilder::withDockerHost);
        applicable = dockerHost.isPresent();
        getSetting("docker.tls.verify").ifPresent(configBuilder::withDockerTlsVerify);
        getSetting("docker.cert.path").ifPresent(configBuilder::withDockerCertPath);
        dockerClientConfig = configBuilder.build();
    }

    private Optional<String> getSetting(final String name) {
        return Optional.ofNullable(TestcontainersConfiguration.getInstance().getEnvVarOrUserProperty(name, null));
    }

    @Override
    public DockerClient getDockerClient(){
        return getClientForConf(getTransportConfig());
    }

    @Override
    public TransportConfig getTransportConfig() {
        return TransportConfig
                .builder()
                .dockerHost(dockerClientConfig.getDockerHost())
                .sslConfig(dockerClientConfig.getSSLConfig())
                .build();
    }

    @Override
    protected int getPriority() {
        return PRIORITY;
    }

    @Override
    public String getDescription() {
        return ("Custom Strategy: environment variables, system properties and defaults. Resolved dockerHost=" +
                dockerClientConfig.getDockerHost());
    }
}
