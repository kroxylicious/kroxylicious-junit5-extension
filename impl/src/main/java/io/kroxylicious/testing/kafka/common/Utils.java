/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.slf4j.LoggerFactory.getLogger;

public class Utils {
    private static final Logger log = getLogger(Utils.class);

    public static void awaitExpectedBrokerCountInCluster(Map<String, Object> connectionConfig, int timeout, TimeUnit timeUnit, Integer expectedBrokerCount) {
        try (Admin admin = Admin.create(connectionConfig)) {
            Awaitility.await()
                    .pollDelay(Duration.ZERO)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .atMost(timeout, timeUnit)
                    .ignoreExceptions()
                    .until(() -> {
                        log.info("describing cluster: {}", connectionConfig.get("bootstrap.servers"));
                        final Collection<Node> nodes;
                        try {
                            nodes = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS);
                            log.info("got nodes: {}", nodes);
                            return nodes;
                        }
                        catch (InterruptedException | ExecutionException e) {
                            log.warn("caught: {}", e.getMessage(), e);
                        }
                        catch (TimeoutException te) {
                            log.warn("Kafka timed out describing the the cluster");
                        }
                        return Collections.emptyList();
                    }, Matchers.hasSize(expectedBrokerCount));
        }
    }
}
