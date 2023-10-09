# CHANGELOG

Please enumerate all user-facing changes using format `<githib issue/pr number>: <short description>`, with changes ordered in reverse chronological order.

## 1.0.0



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
