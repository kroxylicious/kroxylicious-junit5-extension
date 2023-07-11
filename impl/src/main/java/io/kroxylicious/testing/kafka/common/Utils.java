/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.common;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.slf4j.Logger;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * The type Utils.
 */
public class Utils {
    private static final Logger log = getLogger(Utils.class);
    private static final String CONSISTENCY_TEST = "__org_kroxylicious_testing_consistencyTest";

    private Utils() {
    }

    /**
     * Reassign all kafka internal topic partitions that exist have a replica on <code>fromNodeId</code> and don't
     * have at least one replica elsewhere in the cluster.
     *
     * @param connectionConfig the connection config
     * @param fromNodeId nodeId being evacuated
     * @param toNodeId replacement nodeId
     * @param timeout the timeout
     * @param timeUnit the time unit
     */
    public static void awaitReassignmentOfKafkaInternalTopicsIfNecessary(Map<String, Object> connectionConfig, int fromNodeId, int toNodeId, int timeout,
                                                                         TimeUnit timeUnit) {
        var kafkaInternalTopics = List.of(Topic.GROUP_METADATA_TOPIC_NAME, Topic.TRANSACTION_STATE_TOPIC_NAME, Topic.CLUSTER_METADATA_TOPIC_NAME);

        try (var admin = Admin.create(connectionConfig)) {
            awaitCondition(timeout, timeUnit).until(() -> {

                Map<String, TopicDescription> topicDescriptions = describeKnownTopics(kafkaInternalTopics, admin);
                var movements = new HashMap<TopicPartition, Optional<NewPartitionReassignment>>();
                var toNodeReassignment = Optional.of(new NewPartitionReassignment(List.of(toNodeId)));
                topicDescriptions.forEach((name, description) -> {
                    // find all partitions that don't have a replica on at least one other node.
                    var toMove = description.partitions().stream().filter(p -> p.replicas().stream().anyMatch(n -> n.id() == fromNodeId) && p.replicas().size() < 2)
                            .toList();

                    toMove.forEach(tpi -> movements.put(new TopicPartition(name, tpi.partition()), toNodeReassignment));
                });

                if (movements.isEmpty()) {
                    log.debug("No kafka internal topic partitions need re-assigning from node {}", fromNodeId);
                    return true;
                }

                log.debug("Kafka internal topic partitions to re-assign: {}", movements);

                admin.alterPartitionReassignments(movements).all().get();
                return true;
            });

            awaitCondition(timeout, timeUnit)
                    .until(() -> {
                        var ongoingReassignments = admin.listPartitionReassignments().reassignments().get();

                        var ongoingKafkaInternalReassignments = ongoingReassignments.keySet().stream().filter(o -> kafkaInternalTopics.contains(o.topic()))
                                .collect(Collectors.toSet());

                        if (!ongoingKafkaInternalReassignments.isEmpty()) {
                            log.debug("Kafka internal topic partitions re-assigment in-progress: {}", ongoingKafkaInternalReassignments);
                            return false;
                        }
                        log.debug("Kafka internal topic partitions re-assigment complete.");
                        return true;
                    });
        }
    }

    /**
     * Await expected broker count in cluster.
     * Verifies that all expected brokers are present in the cluster.
     * <p>
     * To Verify that all the expected brokers are in the cluster we create a topic with a replication factor = to the expected number of brokers.
     * We then poll describeTopics until
     *
     * @param connectionConfig the connection config
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @param expectedBrokerCount the expected broker count
     */
    public static void awaitExpectedBrokerCountInClusterViaTopic(Map<String, Object> connectionConfig, int timeout, TimeUnit timeUnit, Integer expectedBrokerCount) {
        try (var admin = Admin.create(connectionConfig)) {
            log.debug("Creating topic: {} via {}", CONSISTENCY_TEST, connectionConfig.get(BOOTSTRAP_SERVERS_CONFIG));
            // Note we don't wait for the Topic to be created, we assume it will complete eventually.
            // As the thing we care about is it actually being replicated to the other brokers anyway, so we wait to confirm replication.
            var createTopicStage = createTopic(expectedBrokerCount, admin);
            log.debug("Waiting for {} to be replicated to {} brokers", CONSISTENCY_TEST, expectedBrokerCount);
            awaitCondition(timeout, timeUnit)
                    .failFast(() -> {
                        var f = createTopicStage.toCompletableFuture();
                        if (f.isCompletedExceptionally()) {
                            f.get();
                        }
                    })
                    .until(() -> {
                        log.debug("Calling describe topic");
                        final var promise = new CompletableFuture<Boolean>();
                        admin.describeTopics(Set.of(CONSISTENCY_TEST))
                                .allTopicNames()
                                .whenComplete((topicDescriptions, throwable) -> {
                                    if (throwable != null) {
                                        if (throwable instanceof CompletionException && throwable.getCause() instanceof UnknownTopicOrPartitionException) {
                                            log.debug("Cluster quorum test topic ({}) doesn't exist yet", CONSISTENCY_TEST);
                                        }
                                        else {
                                            log.warn("Unexpected failure describing topic: {} due to {}", CONSISTENCY_TEST, throwable.getMessage(), throwable);
                                        }
                                        promise.complete(false);
                                    }
                                    else {
                                        log.debug("Current topicDescriptions: {}", topicDescriptions);
                                        checkReplicaDistribution(expectedBrokerCount, promise, topicDescriptions);
                                    }
                                })
                                .get(1, TimeUnit.SECONDS);

                        final Boolean isQuorate = promise.getNow(false);
                        if (isQuorate) {
                            deleteTopic(admin);
                        }
                        return isQuorate;
                    });
        }
    }

    /*
     * There are edge cases where deleting the topic isn't possible. Primarily `delete.topic.enable==false`.
     * Rather than attempt to detect that in advance (as that requires an RPC) we catch that and return normally
     * otherwise we rethrow the exception.
     */
    private static void deleteTopic(Admin admin) throws InterruptedException, TimeoutException, ExecutionException {
        try {
            admin.deleteTopics(Set.of(CONSISTENCY_TEST)).all().get(10, TimeUnit.SECONDS);
        }
        catch (ExecutionException ee) {
            if (ee.getCause() instanceof TopicDeletionDisabledException cause) {
                log.warn("Failed to delete {}. Caught: {} ", CONSISTENCY_TEST, cause.getMessage(), cause);
            }
            else {
                throw ee;
            }
        }
        catch (TopicDeletionDisabledException ke) {
            log.warn("caught {} deleting {}", ke.getMessage(), CONSISTENCY_TEST, ke);
        }
    }

    private static void checkReplicaDistribution(Integer expectedBrokerCount, CompletableFuture<Boolean> promise, Map<String, TopicDescription> topicDescriptions) {
        var topicDescription = topicDescriptions.get(CONSISTENCY_TEST);
        if (topicDescription != null) {
            final long distinctReplicas = topicDescription.partitions()
                    .stream()
                    .map(TopicPartitionInfo::replicas)
                    .flatMap(List::stream)
                    .filter(Objects::nonNull)
                    .distinct()
                    .count();
            if (distinctReplicas == expectedBrokerCount) {
                log.debug("Expected number of replicas found.");
                promise.complete(true);
                return;
            }
            log.debug("Unexpected number of replicas found expected: {} got: {}", expectedBrokerCount, distinctReplicas);
        }
        promise.complete(false);
    }

    private static CompletionStage<Void> createTopic(Integer expectedBrokerCount, Admin admin) {
        return admin.createTopics(Set.of(new NewTopic(CONSISTENCY_TEST, 1, expectedBrokerCount.shortValue())))
                .all()
                .toCompletionStage()
                .thenRun(() -> log.debug("Create future for topic {} completed.", CONSISTENCY_TEST))
                .exceptionallyComposeAsync((throwable) -> {
                    log.warn("Failed to create topic: {} due to {}", CONSISTENCY_TEST, throwable.getMessage());
                    if (throwable instanceof CompletionException && throwable.getCause() != null) {
                        throwable = throwable.getCause();
                    }
                    if (throwable instanceof RetriableException || throwable instanceof InvalidReplicationFactorException
                            || (throwable instanceof TopicExistsException && throwable.getMessage().contains("is marked for deletion"))) {
                        // Retry the creation of the topic. The delayed executor used in this stage's handling avoids
                        // a tight spinning loop.
                        return createTopic(expectedBrokerCount, admin);
                    }
                    else {
                        return CompletableFuture.failedStage(new RuntimeException("Failed to create topic: " + CONSISTENCY_TEST, throwable));
                    }
                }, CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS));
    }

    /**
     * Factory for {@link Awaitility#await()} preconfigured with defaults.
     * @param timeout at most timeout
     * @param timeUnit at most {@link TimeUnit}
     * @return preconfigured factory
     */
    public static ConditionFactory awaitCondition(int timeout, TimeUnit timeUnit) {
        return Awaitility.await()
                .pollDelay(Duration.ZERO)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(timeout, timeUnit)
                .ignoreExceptions();
    }

    private static Map<String, TopicDescription> describeKnownTopics(List<String> topics, Admin admin) throws Exception {
        var known = new HashMap<String, TopicDescription>();
        for (String name : topics) {
            try {
                known.putAll(admin.describeTopics(List.of(name)).allTopicNames().get());
            }
            catch (ExecutionException e) {
                // noinspection StatementWithEmptyBody
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    // ignored
                }
                else if (e.getCause() instanceof RuntimeException re) {
                    throw re;
                }
                else {
                    throw new RuntimeException(e.getCause());
                }
            }
        }

        return known;
    }

}
