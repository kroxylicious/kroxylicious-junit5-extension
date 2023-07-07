/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
public class BeforeEachTest {

    @BeforeEach
    public void setup(@Name("a") KafkaCluster cluster, @Name("a") Admin admin) throws Exception {
        admin.createTopics(List.of(new NewTopic("topicName", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
    }

    @Test
    public void test(@Name("a") KafkaCluster cluster, @Name("a") Admin admin) throws Exception {
        var topicListings = await().atMost(10, TimeUnit.SECONDS).until(() -> admin.listTopics().listings().get(10, TimeUnit.SECONDS),
                listings -> !listings.isEmpty());

        assertEquals(1, topicListings.size());
        assertEquals("topicName", topicListings.iterator().next().name());
    }
}
