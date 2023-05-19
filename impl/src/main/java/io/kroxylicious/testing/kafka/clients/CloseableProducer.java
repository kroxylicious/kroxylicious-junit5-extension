/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.clients;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

/**
 * Provides a simple wrapper around a Kafka Producer to redirect `close()`
 * so that it has a sensible timeout and can thus be safely used in a try-with-resources block.
 * All other methods delegate to the wrapped Producer
 *
 * @param <K>  the type parameter
 * @param <V>  the type parameter
 * @param instance the instance
 */
public record CloseableProducer<K, V>(Producer<K, V> instance) implements Producer<K, V>, AutoCloseable {

    /**
     * Wrap producer.
     *
     * @param <K>  the type parameter
     * @param <V>  the type parameter
     * @param instance the instance
     * @return the producer
     */
    public static <K, V> Producer<K, V> wrap(Producer<K, V> instance) {
        return new CloseableProducer<>(instance);
    }

    /**
     * Create producer.
     *
     * @param <K>  the type parameter
     * @param <V>  the type parameter
     * @param properties   The producer configs
     * @return the producer
     */
    public static <K, V> Producer<K, V> create(Properties properties) {
        return wrap(new KafkaProducer<>(properties));
    }

    /**
     * Create producer.
     *
     * @param <K>  the type parameter
     * @param <V>  the type parameter
     * @param configs   The producer configs
     * @return the producer
     */
    public static <K, V> Producer<K, V> create(Map<String, Object> configs) {
        return wrap(new KafkaProducer<>(configs));
    }

    @Override
    public void close() {
        instance.close(Duration.ofSeconds(5L));
    }

    @Override
    public void initTransactions() {
        instance.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        instance.beginTransaction();
    }

    @Override
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        instance.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        instance.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        instance.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        instance.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return instance.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return instance.send(record, callback);
    }

    @Override
    public void flush() {
        instance.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return instance.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return instance.metrics();
    }

    @Override
    public void close(Duration timeout) {
        instance.close(timeout);
    }
}
