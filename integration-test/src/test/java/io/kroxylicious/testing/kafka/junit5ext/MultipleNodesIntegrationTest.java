/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KRaftCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
class MultipleNodesIntegrationTest {

    @Test
    void shouldInjectClusterWithMultipleBrokers(@KRaftCluster(numControllers = 3) KafkaCluster cluster) {
        try (Admin admin = Admin.create(cluster.getKafkaClientConfiguration())) {
            // When
            final DescribeClusterResult describeClusterResult = admin.describeCluster();

            // Then
            await().atMost(30, TimeUnit.SECONDS).until(() -> describeClusterResult.nodes().isDone());
            assertThat(describeClusterResult.nodes()).isNotCancelled();
            assertThat(describeClusterResult.controller()).isNotCancelled();
        }
    }
}
