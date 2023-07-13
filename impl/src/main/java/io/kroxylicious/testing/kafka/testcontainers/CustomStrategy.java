package io.kroxylicious.testing.kafka.testcontainers;

import java.time.Duration;
import java.util.Optional;

import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.dockerclient.TransportConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.DefaultDockerClientConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.DefaultDockerCmdExecFactory;
import org.testcontainers.shaded.com.github.dockerjava.core.DockerClientConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.DockerClientImpl;
import org.testcontainers.utility.TestcontainersConfiguration;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient;

import lombok.Getter;

public class CustomStrategy extends DockerClientProviderStrategy {

    private static final System.Logger LOGGER = System.getLogger(CustomStrategy.class.getName());

    public static final int PRIORITY = 100;

    private final DockerClientConfig dockerClientConfig;

    @Getter(lazy = true)
    private final DockerClient dockerClient = getClientForConfig(getTransportConfig());

    public CustomStrategy() {
        LOGGER.log(System.Logger.Level.WARNING, "using CustomStrategy");
        // use docker-java defaults if present, overridden if our own configuration is set
        DefaultDockerClientConfig.Builder configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder();

        getSetting("docker.host").ifPresent(configBuilder::withDockerHost);
        getSetting("docker.tls.verify").ifPresent(configBuilder::withDockerTlsVerify);
        getSetting("docker.cert.path").ifPresent(configBuilder::withDockerCertPath);

        dockerClientConfig = configBuilder.build();
    }

    public static DockerClient getClientForConfig(TransportConfig transportConfig) {
        final DockerHttpClient dockerHttpClient = new ZerodepDockerHttpClient.Builder()
                .maxConnections(0)
                .connectionTimeout(Duration.ofSeconds(5))
                .responseTimeout(Duration.ofSeconds(5))
                .dockerHost(transportConfig.getDockerHost())
                .sslConfig(transportConfig.getSslConfig())
                .build();
        final DockerClientImpl dockerClient = (DockerClientImpl) DockerClientProviderStrategy.getClientForConfig(transportConfig);
        final DefaultDockerCmdExecFactory dockerCmdExecFactory = new DefaultDockerCmdExecFactory(dockerHttpClient, DockerClientConfig.getDefaultObjectMapper());
        return dockerClient.withDockerCmdExecFactory(dockerCmdExecFactory);
    }

    @Override
    public TransportConfig getTransportConfig() {
        return TransportConfig.builder()
                .dockerHost(dockerClientConfig.getDockerHost())
                .sslConfig(dockerClientConfig.getSSLConfig())
                .build();
    }

    @Override
    public String getDescription() {
        return "Environment variables, system properties and defaults. Resolved dockerHost=" + dockerClientConfig.getDockerHost();
    }

    @Override
    protected boolean isApplicable() {
        return getSetting("docker.host").isPresent();
    }

    @Override
    protected int getPriority() {
        return PRIORITY;
    }

    private Optional<String> getSetting(final String name) {
        return Optional.ofNullable(TestcontainersConfiguration.getInstance().getEnvVarOrUserProperty(name, null));
    }
}
