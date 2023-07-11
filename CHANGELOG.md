# CHANGELOG

Please enumerate all user-facing changes using format `<githib issue/pr number>: <short description>`, with changes ordered in reverse chronological order.

## 1.0.0

## 0.5.0

* [#145](https://github.com/kroxylicious/kroxylicious-junit5-extension/pull/145) Direct storage formatting stdout to the log
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
