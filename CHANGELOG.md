# CHANGELOG

Please enumerate all user-facing changes using format `<githib issue/pr number>: <short description>`, with changes ordered in reverse chronological order.

## 1.0.0

## SNAPSHOT
## 0.9.1

* [#365](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/365): Bump kafka.version from 3.7.1 to 3.8.0

## 0.9.0

* [#355](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/355): Changes for Kafka 3.8.0 compatibility
* [#322](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/322): Allow test Kafka Clusters to use SASL SCRAM-SHA and OAUTH bearer
* [#289](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/289): Bump org.testcontainers:testcontainers-bom from 1.19.6 to 1.19.7
* [#283](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/283): Bump kafka.version from 3.6.1 to 3.7.0.

### Changes, deprecations and removals

* For SCRAM support in KRaft mode,  the Kafka Broker must be 3.5 or above and it must use storage format IBP_3_5_IV2
  or higher.  If using CONTAINER mode, be aware that support for this combination was introduced in
  [kafka-native 0.9.0](https://github.com/ozangunalp/kafka-native/releases/tag/v0.9.0).
* In KRaft mode, the metadata storage format used by the test clusters defaults to `MetadataVersion.LATEST_PRODUCTION`
  rather than `MINIMUM_BOOTSTRAP_VERSION` as was the case in previous releases.  To get back the old behaviour, override
  the `inter.broker.protocol.version` broker configuration option and set the desired version. This config option
  also controls the metadata storage format when the storage is formatted for the first time.

## 0.8.1

* [#262](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/262): Support embedding a 3.7 broker

## 0.8.0

* [#250](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/250): Bump Apache Kafka to 3.6.1
* [#248](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/248): Allow user to bring their own test templating when using KafkaClusterExtension
* [#244](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/244): Bump org.testcontainers:testcontainers-bom from 1.19.2 to 1.19.3

## 0.7.0

* [#221](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/221): Add ability to inject test topics into tests #229
* [#231](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/231): Bump org.testcontainers:testcontainers-bom from 1.19.1 to 1.19.2
* [#219](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/219): Allow kafka configuration to be provided for Producers, Consumer and Admin clients too
* [#143](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/143): Default native kafka image tag from the version of Kafka on the classpath.
* [#197](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/197): Consumer of the extension should be able to choose the version of the kafka dependency.


### Changes, deprecations and removals

* It is now necessary to declare `org.apache.kafka:kafka_2.13` as a test dependency.
* If you wish to use kafka in-vm with zookeeper, `org.apache.zookeeper:zookeeper` must be declared as test dependency too.
* The way `TestcontainersKafkaCluster` determines the native image version tag to use for kafka and zookeeper is changed.
  Previously `TestcontainersKafkaCluster` always used the `latest-snapshot` version.  With this release, it will now default
  to use the version of the kafka native image that corresponds to the version of Kafka Broker found on the classpath.
  This will give consumers of the test extension that have tests using `TestcontainersKafkaCluster` test repeatability
  and consumers who use both `InVMKafkaCluster` and `TestcontainersKafkaCluster` kafka broker version consistency.
  To get back the original behaviour, annotate the  `KafkaCluster` with `@Version("latest-snapshot")`.
* The annotation `@ClientConfig` is now available to pass configuration to Kafka Clients.  This is
  understood by `Producer`, `Consumer`, and `Admin` types.

## 0.6.0

* [#193](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/193): Bump Apache Kafka from 3.5.1 to 3.6.0
* [#190](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/190): Allow controller-only nodes in container mode
* [#187](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/187): Tighten the conditions under which the extension will inject values.
* [#186](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/186): Replace a Null Pointer Exception with a more meaningful one when there is no existing cluster
* [#182](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/182): Prevent possibility of loop when performing consistency test (workaround KAFKA-15507)
* [#180](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/180): Update kafka version range in testVersions to include 3.5.1

## 0.5.0

* [#146](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/146): Expose TestInfo (and thus DisplayName) when clusters are injected via  annotations.
* [#145](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/145) Direct storage formatting stdout to the log
* [#86](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/86) Added API to restart one or more brokers of a kafka cluster.
* [#142](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/142) Prevent spinning in Utils#createTopic.
* [#140](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/140) Prevent docker networks accumulations during test runs.
* [#134](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/134): Support @BeforeEach and test cases referring to same cluster
* [#130](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/130): Small KeytoolCertificateGenerator improvements (eliminates use of openssl)

### Changes, deprecations and removals

* It is now legal to declare multiple KafkaCluster parameters with the same explicit name, as long as later declarations only supply an @Name annotation and no constraint annotations. This will inject a reference to the existing cluster.

## 0.4.0

* [#127](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/127): Upgrade to kafka-clients 3.5.0
* [#122](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/122): Bump maven-failsafe-plugin from 3.1.0 to 3.1.2
* [#114](https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/114): Fix defect that allowed the consistencyTest topic to be visible to users of the extension. 
* [#116](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/116): Bump lombok from 1.18.26 to 1.18.28
* [#115](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/115): Bump maven-dependency-plugin from 3.5.0 to 3.6.0
