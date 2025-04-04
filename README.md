# Kafka Cluster JUnit 5 extension

## What?

This is a JUnit 5 extension that allows writing tests that require a Kafka cluster
without having to hard-code exactly _how_ that cluster is provisioned.
As such it allows you to write very flexible tests that abstract over different kinds of cluster.

## Dependency
Current version: ![Maven Central Version](https://img.shields.io/maven-central/v/io.kroxylicious.testing/testing-junit5-extension)

Gradle 
```
testImplementation 'io.kroxylicious.testing:testing-junit5-extension:0.1'
```

Maven

```xml
<dependencies>
    <dependency>
        <groupId>io.kroxylicious.testing</groupId>
        <artifactId>testing-junit5-extension</artifactId>
        <version>0.1</version>
    </dependency>
    <!-- Kafka Broker version you want to use -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka_2.13</artifactId>
        <version>${kafka.version}</version> <!-- versions from 3.3.0 have been tested  -->
        <scope>test</scope>
    </dependency>
    <!-- Optional, required if you want to use in-VM kafka in Zookeeper mode. -->
    <dependency>
        <groupId>org.apache.zookeeper</groupId>
        <artifactId>zookeeper</artifactId>
        <version>${zookeeper.version}</version> <!-- versions from 3.6.3 have been tested -->
        <scope>test</scope>
    </dependency>
</dependencies>
```

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
* `@SaslMechanism` will provide cluster with the external listener configured for the given SASL
   mechanism.  Use of this option requires the client to use SASL authentication. For PLAIN and SCRAM mechanism a
   database of principals must be provided.
* `@Version(value="3.3.1")` will provide a container-based cluster with the kafka/zookeeper version indicated

When multiple constraints are provided they will _all_ be satisfied.

The cluster will be provisioned using the fastest available mechanism, because your development inner loop is a precious thing.

## Configuring Kafka Brokers

To configure a kafka broker, apply the `@BrokerConfig` annotation to the `KafkaCluster` type.

For example:

```java
@ExtendWith(KafkaClusterExtension.class)
class MyTest {
    @BrokerConfig(name = "compression.type", value = "zstd") KafkaCluster cluster;

    // ...
}
```

## Configuring SASL

To config the Broker to use SASL, use the annotations `@SaslMechanism` to specify the SASL
mechanism and, if required, a database of principals.

```java
class MyTest {
@SaslMechanism(value = "PLAIN", principals = { @Principal(user = "alice", password = "foo") }) KafkaCluster cluster;

    // ...
}
```

## Configuring Kafka Clients

To configure kafka clients, apply the `@ClientConfig` annotation.  It can be applied to the following types:

* `Producer`
* `Consumer`
* `Admin`

For example:

```java
@ExtendWith(KafkaClusterExtension.class)
class MyTest {
    @ClientConfig(name = "client.id", value = "myclient") Producer<String, String> producer;

    // ...
}
```

## Creating Test Topics

The test framework can create topic(s) for the test.  It is
guaranteed that the test will exist before the test method is invoked.

You can also specify configuration for the topic using the annotation `@TopicConfig`,
or set its partition count or replication factor with `@TopicPartitions` and `@TopicReplicationFactor`
respectively.

```java
@ExtendWith(KafkaClusterExtension.class)
class MyTest {
    Topic myTopic;
    @TopicConfig(name = "cleanup.policy", value = "compact") Topic myTopicWithConfig;
    // ...
}
```


## Node topology

When generating a cluster using KRaft (the default), you declare how many brokers and controllers you want and the extension will provision the minimum number of nodes to satisfy those conditions. It will create as many nodes as it can that are both KRaft controllers and brokers using [process.roles](https://kafka.apache.org/documentation/#brokerconfigs_process.roles) (process.roles = "broker,controller"). For example:


| numBrokers | numControllers | roles                                                               |
|------------|----------------|---------------------------------------------------------------------|
| 1          | 1              | `"broker,controller"`                                               |
| 3          | 1              | `"broker,controller"`, `"broker"`, `"broker"`                       |
| 1          | 3              | `"broker,controller"`, `"controller"`, `"controller"`               |
| 3          | 3              | `"broker,controller"`, `"broker,controller"`, `"broker,controller"` |

## Provisioning mechanisms

The following provisioning mechanisms are currently supported:

* an in-VM Kafka cluster (by creating a `KafkaServer` within the same JVM as your tests)
* a container-based cluster using [testcontainers](https://www.testcontainers.org/).

Which kind of cluster is chosen depends on the requirements of your test.
For example, using containers allows to easily test against different broker versions. 

---
**NOTE**

In case you use podman for testcontainers, some tips must be taken into account:
* `TESTCONTAINERS_RYUK_DISABLED=true` env variable shall be declared as per https://github.com/containers/podman/issues/7927#issuecomment-731525556
* `podman-plugins` linux package shall be installed on your machine in order to communicate containers among each other.

---

## Field injection and Parameter Resolution

The extension supports injecting clusters and clients:
- into fields of the test class 
- as parameters to `@BeforeAll`
- as parameters to `@BeforeEach`
- as parameters to test methods

To avoid collisions with other extensions, such as Mockito, we will only inject into fields which:
- have no annotations  
OR are annotated with annotations from the following packages
- `io.kroxylicious`
- `org.junit`
- `java.lang`

The extension will not try to overwrite fields which have already been initialised. 

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

## Custom cluster provisioning and constraints

Provisioning mechanisms can be provided externally by implementing `io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy` and declaring it as a [Java service](https://www.baeldung.com/java-spi) (e.g. in a  `META-INF/services/io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy` file that is available on the test-time classpath).

You can also provide your own constraint annotation types by annotating them with the `@io.kroxylicious.testing.kafka.api.KafkaClusterConstraint` meta-annotation. Such custom constraint annotations will only be understood by your custom provisioning strategy, so the in-JVM and testcontainers-based clusters provided by this project then can't be used.

## Developer Guide

See the [developer guide](DEV_GUIDE.md).

## Releasing this project

See the [releasing guide](RELEASING.md).
