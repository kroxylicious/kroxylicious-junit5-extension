/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.clients;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * Provides a simple wrapper around a Kafka Consumer to redirect `close()`
 * so that it has a sensible timeout and can thus be safely used in a try-with-resources block.
 * All other methods delegate to the wrapped Consumer client.
 *
 * @param instance the consumer instance
 * @param <K>  the type parameter
 * @param <V>  the type parameter
 */
public record CloseableConsumer<K, V>(Consumer<K, V> instance) implements Consumer<K, V>, AutoCloseable {
    /**
     * Wrap consumer.
     *
     * @param <K>  the type parameter
     * @param <V>  the type parameter
     * @param instance the instance
     * @return the consumer
     */
    public static <K, V> Consumer<K, V> wrap(Consumer<K, V> instance) {
        return new CloseableConsumer<>(instance);
    }

    /**
     * Create consumer.
     *
     * @param <K>  the type parameter
     * @param <V>  the type parameter
     * @param properties   The consumer configs
     * @return the consumer
     */
    public static <K, V> Consumer<K, V> create(Properties properties) {
        return wrap(new KafkaConsumer<>(properties));
    }

    /**
     * Create consumer.
     *
     * @param <K>  the type parameter
     * @param <V>  the type parameter
     * @param configs   The consumer configs
     * @return the consumer
     */
    public static <K, V> Consumer<K, V> create(Map<String, Object> configs) {
        return wrap(new KafkaConsumer<>(configs));
    }

    @Override
    public Set<TopicPartition> assignment() {
        return instance.assignment();
    }

    @Override
    public Set<String> subscription() {
        return instance.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        instance.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        instance.subscribe(topics, callback);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        instance.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        instance.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        instance.subscribe(pattern);
    }

    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
        instance.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(SubscriptionPattern pattern) {
        instance.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        instance.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return instance.poll(timeout);
    }

    @Override
    public void commitSync() {
        instance.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        instance.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        instance.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        instance.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        instance.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        instance.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        instance.commitAsync(offsets, callback);
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        instance.registerMetricForSubscription(metric);
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        instance.unregisterMetricFromSubscription(metric);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        instance.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        instance.seek(partition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        instance.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        instance.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return instance.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return instance.position(partition, timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return instance.committed(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return instance.committed(partitions, timeout);
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return instance.clientInstanceId(timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return instance.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return instance.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return instance.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return instance.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return instance.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return instance.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        instance.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        instance.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return instance.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return instance.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return instance.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return instance.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return instance.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return instance.endOffsets(partitions, timeout);
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return instance.currentLag(topicPartition);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return instance.groupMetadata();
    }

    @Override
    public void enforceRebalance() {
        instance.enforceRebalance();
    }

    @Override
    public void enforceRebalance(String reason) {
        instance.enforceRebalance(reason);
    }

    @Override
    public void close() {
        instance.close(Duration.ofSeconds(5L));
    }

    @Override
    public void close(Duration timeout) {
        instance.close(timeout);
    }

    @Override
    public void wakeup() {
        instance.wakeup();
    }
}
