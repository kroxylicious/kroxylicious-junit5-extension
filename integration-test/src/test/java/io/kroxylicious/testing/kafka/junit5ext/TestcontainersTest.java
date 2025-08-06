/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.common.Version;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class TestcontainersTest {

    @Test
    public void test39(@Version("3.9.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    public void test40(@Version("4.0.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    public void test41(@Version("4.1.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    public void testLatest(@Version(Version.LATEST_RELEASE) TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }
}
