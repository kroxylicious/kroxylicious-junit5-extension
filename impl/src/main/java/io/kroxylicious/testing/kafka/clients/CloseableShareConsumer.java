/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.clients;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * Provides a simple wrapper around a Kafka ShareConsumer to redirect `close()`
 * so that it has a sensible timeout and can thus be safely used in a try-with-resources block.
 * All other methods delegate to the wrapped ShareConsumer client.
 *
 * @param instance the consumer instance
 * @param <K>  the type parameter
 * @param <V>  the type parameter
 */
public record CloseableShareConsumer<K, V>(ShareConsumer<K, V> instance) implements ShareConsumer<K, V>, AutoCloseable {
    @Override
    public Set<String> subscription() {
        return instance.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        instance.subscribe(topics);
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
    public void acknowledge(ConsumerRecord<K, V> record) {
        instance.acknowledge(record);
    }

    @Override
    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        instance.acknowledge(record, type);
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return instance.commitSync();
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
        return instance.commitSync(timeout);
    }

    @Override
    public void commitAsync() {
        instance.commitAsync();
    }

    @Override
    public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
        instance.setAcknowledgementCommitCallback(callback);
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
    public void registerMetricForSubscription(KafkaMetric metric) {
        instance.registerMetricForSubscription(metric);
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        instance.unregisterMetricFromSubscription(metric);
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
