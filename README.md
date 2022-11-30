# Kafka Cluster JUnit 5 extension

## What?

This is a JUnit 5 extension that allows writing tests that require a Kafka cluster
without having to hard-code exactly _how_ that cluster is provisioned.
As such it allows you to write very flexible tests that abstract over different kinds of cluster.

## Example

Here's a minimal example:

```java
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(KafkaClusterExtension.class) // <1>
class MyTest {

    KafkaCluster cluster; // <2>

    @Test
    public void testProducer(
            Producer<String, String> producer // <3>
    ) throws Exception {
        producer.send(new ProducerRecord<>("hello", "world")).get();
    }
}
```

Notes:

1. You have to tell Junit that you're using the extension using `@ExtendWith`.
2. An instance field of type `KafkaCluster` will cause a new cluster to be provisioned for each test in the class. Alternatively you could use a parameter on a `@Test`-annotated method. If you use a `static` field then a single cluster will be provisioned for _all_ the tests in the class.
3. Your test methods can declare `Producer`, `Consumer` and `Admin`-typed parameters. They will be configured to bootstrap against the `cluster`.

By default, this would use a single-broker KRaft-based Kafka cluster running in the same VM as your test.

## Adding Constraints

Such a cluster might not be what you need for the scenario you're trying to test.
You can configure different clusters by annotating the `KafkaCluster` field or parameter:

* `@BrokerCluster(numBrokers=3)` will use a Kafka cluster with the given number of brokers
* `@KRaftCluster` will ensure a KRaft-based cluster is used. `@KRaftCluster(numControllers=3)` will use a controller quorum with 3 controllers.
* `@ZooKeeperCluster` will ensure a ZooKeeper-based Kafka cluster (unsurprisingly this is mutually exclusive with `@KRaftCluster`)
* `@SaslPlainAuth` will provide cluster with `SASL-PLAIN` authentication.

When multiple constraints are provided they will _all_ be satisfied.

The cluster that will be provisioned using the fastest available mechanism, because your development inner loop is a precious thing.

## Provisioning mechanisms

The following provisioning mechanisms are currently supported:

* an in-VM Kafka cluster (by creating a `KafkaServer` within the same JVM as your tests)
* a container-based cluster using [testcontainers](https://www.testcontainers.org/).

Which kind of cluster is chosen depends on the requirements of your test.
For example, using containers allows to easily test against different broker versions. 

Other provisioning mechanisms will be added in the future (e.g. provisioning a cluster within Kubernetes using [Strimzi](https://strimzi.io/)).

## Template tests

You can also use test templates to execute the same test over a number of different cluster configurations. Here's an example:

```java
import java.util.stream.Stream;

import io.kroxylicious.testing.kafka.common.ConstraintUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.DimensionMethodSource;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.kroxylicious.junit5.constraint.ConstraintUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KafkaClusterExtension.class) // 1
public class TemplateTest {
    @TestTemplate // <2>
    public void multipleClusterSizes(
            @DimensionMethodSource("clusterSizes") // <3>
            KafkaCluster cluster) throws Exception {
        // ... your test code
    }

    static Stream<BrokerCluster> clusterSizes() {// 4
        return Stream.of(
                ConstraintUtils.brokerCluster(1),
                ConstraintUtils.brokerCluster(3));
    }
}
```

1. You have to tell Junit that you're using the extension using `@ExtendWith`.
2. You use [JUnit Jupiter's `@TestTemplate`](https://junit.org/junit5/docs/current/user-guide/#writing-tests-test-templates) rather than a `@Test` to tell JUnit that this method is a _factory_ for tests.
3. You provide a reference to source of additional constraints. Here the source is a method named via the `@DimensionMethodSource` annotation.
4. The method must be `static`, take no parameters and return a `Stream`, `Collection` or array of constraint annotations. 

A test will be executed as many times as there are constraints in the stream with a different Kafka cluster each time. These constraints are _in addition to_ any constraint annotations applied to the `KafkaCluster cluster` parameter of the `multipleClusterSizes()` method. (There are none in the example above, but we could have applied any from the above list).

You can use multiple instances of `@DimensionMethodSource`. In that case every combination of constraints will be tested (i.e. the full cartesian product over all the constraint sources). For example, we could write:

```java
import java.util.stream.Stream;

import io.kroxylicious.testing.kafka.common.ConstraintUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.DimensionMethodSource;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import org.apache.kafka.clients.admin.Admin;

import static io.kroxylicious.junit5.constraint.ConstraintUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KafkaClusterExtension.class)
public class TemplateTest {
    @TestTemplate
    public void multipleClusterSizes(
            @DimensionMethodSource("clusterSizes")
            @DimensionMethodSource("compression")
            KafkaCluster cluster,
            Admin admin) throws Exception {
        // ...
    }

    static Stream<BrokerConfig> compression() {
        return Stream.of(
                ConstraintUtils.brokerConfig("compression.type", "zstd"),
                ConstraintUtils.brokerConfig("compression.type", "snappy"));
    }
}
```
  And the template would produce the following test combinations:

* 1 broker with `compression.type=zstd`
* 3 brokers with `compression.type=zstd`
* 1 broker with `compression.type=snappy`
* 3 brokers with `compression.type=snappy`

Alternatively if you don't need to test _every_ combination, you could use a `@ConstraintsMethodSource` source that returns just those combinations that you _do_ want to test. In this case the return type of the source method must be `Stream<List<Annotation>>`, `Collection<List<Annotation>>` or `List<Annotation>[]`.

```java
import java.util.stream.Stream;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import org.apache.kafka.clients.admin.Admin;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.ConstraintsMethodSource;

import static io.kroxylicious.junit5.constraint.ConstraintUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KafkaClusterExtension.class)
public class TemplateTest {
    @TestTemplate
    public void testTuples(@ConstraintsMethodSource("tuples")
                           KafkaCluster cluster,
                           Admin admin)
            throws Exception {
        // ...
    }

    static Stream<List<Annotation>> tuples() {
        return Stream.of(
                List.of(brokerCluster(1), kraftCluster(1)),
                List.of(brokerCluster(3), kraftCluster(1)),
                List.of(brokerCluster(3), zooKeeperCluster()));
    }
}
```

This will produce tests as follows:

* 1 broker, KRaft-based
* 3 brokers, KRaft-based
* 3 brokers, ZK-based
