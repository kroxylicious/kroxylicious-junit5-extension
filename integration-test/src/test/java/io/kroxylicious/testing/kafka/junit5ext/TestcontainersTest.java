/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.ReflectionUtils;

import io.kroxylicious.testing.kafka.common.Version;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
class TestcontainersTest {

    @Test
    void test39(@Version("3.9.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    void test40(@Version("4.0.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    void test41(@Version("4.1.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    void test42(@Version("4.2.0") TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    @Test
    void testLatest(@Version(Version.LATEST_RELEASE) TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterId = cluster.createAdmin().describeCluster().clusterId().get(10, TimeUnit.SECONDS);
        assertThat(clusterId).isNotEmpty();
    }

    /**
     * This test is here to check that the current kafka version on the classpath is covered
     */
    @Test
    void testCurrentVersion() {
        var versionFromClasspath = AppInfoParser.getVersion();
        List<Method> methods = ReflectionUtils.findMethods(TestcontainersTest.class,
                method -> method.getParameterCount() == 1 && method.getParameterTypes()[0] == TestcontainersKafkaCluster.class
                        && method.getParameters()[0].isAnnotationPresent(Version.class));
        List<String> allTestedVersions = methods.stream().flatMap(method -> Stream.of(method.getParameters()[0].getAnnotationsByType(Version.class))).map(Version::value)
                .sorted()
                .toList();
        assertThat(allTestedVersions).withFailMessage("kafka version '" + versionFromClasspath + "' from classpath not found in tested versions " + allTestedVersions)
                .contains(versionFromClasspath);
    }
}
