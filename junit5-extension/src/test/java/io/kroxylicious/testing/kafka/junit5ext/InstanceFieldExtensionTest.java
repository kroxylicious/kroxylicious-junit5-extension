/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.invm.InVMKafkaCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(KafkaClusterExtension.class)
public class InstanceFieldExtensionTest extends AbstractExtensionTest {

    @BrokerCluster(numBrokers = 1)
    KafkaCluster instanceCluster;

    @Test
    public void clusterInstanceField()
            throws ExecutionException, InterruptedException {
        var dc = describeCluster(instanceCluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertEquals(instanceCluster.getClusterId(), dc.clusterId().get());
        var cbc = assertInstanceOf(InVMKafkaCluster.class, instanceCluster);
    }

    @Test
    public void adminParameter(Admin admin) throws ExecutionException, InterruptedException {
        assertSameCluster(instanceCluster, admin);
    }
}
