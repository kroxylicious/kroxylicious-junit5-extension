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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AddRaftVoterOptions;
import org.apache.kafka.clients.admin.AddRaftVoterResult;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupsResult;
import org.apache.kafka.clients.admin.DeleteStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClassicGroupsOptions;
import org.apache.kafka.clients.admin.DescribeClassicGroupsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesOptions;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesResult;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.ListConfigResourcesResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RemoveRaftVoterOptions;
import org.apache.kafka.clients.admin.RemoveRaftVoterResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.admin.TerminateTransactionOptions;
import org.apache.kafka.clients.admin.TerminateTransactionResult;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

/**
 * Provides a simple wrapper around a Kafka Admin client to redirect `close()`
 * so that it has a sensible timeout and can thus be safely used in a try-with-resources block.
 * All other methods delegate to the wrapped Admin client.
 * @param instance the admin instance
 */
public record CloseableAdmin(Admin instance) implements Admin, AutoCloseable {

    /**
     * Wrap admin.
     *
     * @param instance the instance
     * @return the admin
     */
    public static Admin wrap(Admin instance) {
        return new CloseableAdmin(instance);
    }

    @Override
    public void close() {
        instance.close(Duration.ofSeconds(5L));
    }

    /**
     * Create admin.
     *
     * @param props the props
     * @return the admin
     */
    public static Admin create(Properties props) {
        return wrap(Admin.create(props));
    }

    /**
     * Create admin.
     *
     * @param conf the conf
     * @return the admin
     */
    public static Admin create(Map<String, Object> conf) {
        return wrap(Admin.create(conf));
    }

    @Override
    public void close(Duration timeout) {
        instance.close(timeout);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return instance.createTopics(newTopics);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        return instance.createTopics(newTopics, options);
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return instance.deleteTopics(topics);
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
        return instance.deleteTopics(topics, options);
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics) {
        return instance.deleteTopics(topics);
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options) {
        return instance.deleteTopics(topics, options);
    }

    @Override
    public ListTopicsResult listTopics() {
        return instance.listTopics();
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return instance.listTopics(options);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return instance.describeTopics(topicNames);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        return instance.describeTopics(topicNames, options);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topics) {
        return instance.describeTopics(topics);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
        return instance.describeTopics(topics, options);
    }

    @Override
    public DescribeClusterResult describeCluster() {
        return instance.describeCluster();
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        return instance.describeCluster(options);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter) {
        return instance.describeAcls(filter);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        return instance.describeAcls(filter, options);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls) {
        return instance.createAcls(acls);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        return instance.createAcls(acls, options);
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters) {
        return instance.deleteAcls(filters);
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        return instance.deleteAcls(filters, options);
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources) {
        return instance.describeConfigs(resources);
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        return instance.describeConfigs(resources, options);
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        return instance.incrementalAlterConfigs(configs);
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
        return instance.incrementalAlterConfigs(configs, options);
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment) {
        return instance.alterReplicaLogDirs(replicaAssignment);
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options) {
        return instance.alterReplicaLogDirs(replicaAssignment, options);
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers) {
        return instance.describeLogDirs(brokers);
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        return instance.describeLogDirs(brokers, options);
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas) {
        return instance.describeReplicaLogDirs(replicas);
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        return instance.describeReplicaLogDirs(replicas, options);
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions) {
        return instance.createPartitions(newPartitions);
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        return instance.createPartitions(newPartitions, options);
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
        return instance.deleteRecords(recordsToDelete);
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        return instance.deleteRecords(recordsToDelete, options);
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken() {
        return instance.createDelegationToken();
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        return instance.createDelegationToken(options);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac) {
        return instance.renewDelegationToken(hmac);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        return instance.renewDelegationToken(hmac, options);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac) {
        return instance.expireDelegationToken(hmac);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        return instance.expireDelegationToken(hmac, options);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken() {
        return instance.describeDelegationToken();
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        return instance.describeDelegationToken(options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        return instance.describeConsumerGroups(groupIds, options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
        return instance.describeConsumerGroups(groupIds);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return instance.listConsumerGroups(options);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups() {
        return instance.listConsumerGroups();
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options) {
        return instance.listConsumerGroupOffsets(groupId, options);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return instance.listConsumerGroupOffsets(groupId);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        return instance.listConsumerGroupOffsets(groupSpecs, options);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs) {
        return instance.listConsumerGroupOffsets(groupSpecs);
    }

    @Override
    public ListStreamsGroupOffsetsResult listStreamsGroupOffsets(Map<String, ListStreamsGroupOffsetsSpec> map,
                                                                 ListStreamsGroupOffsetsOptions listStreamsGroupOffsetsOptions) {
        return instance.listStreamsGroupOffsets(map, listStreamsGroupOffsetsOptions);
    }

    @Override
    public ListStreamsGroupOffsetsResult listStreamsGroupOffsets(Map<String, ListStreamsGroupOffsetsSpec> groupSpecs) {
        return instance.listStreamsGroupOffsets(groupSpecs);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        return instance.deleteConsumerGroups(groupIds, options);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds) {
        return instance.deleteConsumerGroups(groupIds);
    }

    @Override
    public DeleteStreamsGroupsResult deleteStreamsGroups(Collection<String> collection, DeleteStreamsGroupsOptions deleteStreamsGroupsOptions) {
        return instance.deleteStreamsGroups(collection, deleteStreamsGroupsOptions);
    }

    @Override
    public DeleteStreamsGroupsResult deleteStreamsGroups(Collection<String> groupIds) {
        return instance.deleteStreamsGroups(groupIds);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        return instance.deleteConsumerGroupOffsets(groupId, partitions, options);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions) {
        return instance.deleteConsumerGroupOffsets(groupId, partitions);
    }

    @Override
    public DeleteStreamsGroupOffsetsResult deleteStreamsGroupOffsets(String s, Set<TopicPartition> set,
                                                                     DeleteStreamsGroupOffsetsOptions deleteStreamsGroupOffsetsOptions) {
        return instance.deleteStreamsGroupOffsets(s, set, deleteStreamsGroupOffsetsOptions);
    }

    @Override
    public DeleteStreamsGroupOffsetsResult deleteStreamsGroupOffsets(String groupId, Set<TopicPartition> partitions) {
        return instance.deleteStreamsGroupOffsets(groupId, partitions);
    }

    @Override
    public ListGroupsResult listGroups(ListGroupsOptions options) {
        return instance.listGroups(options);
    }

    @Override
    public ListGroupsResult listGroups() {
        return instance.listGroups();
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions) {
        return instance.electLeaders(electionType, partitions);
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions, ElectLeadersOptions options) {
        return instance.electLeaders(electionType, partitions, options);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
        return instance.alterPartitionReassignments(reassignments);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
                                                                         AlterPartitionReassignmentsOptions options) {
        return instance.alterPartitionReassignments(reassignments, options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments() {
        return instance.listPartitionReassignments();
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Set<TopicPartition> partitions) {
        return instance.listPartitionReassignments(partitions);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Set<TopicPartition> partitions, ListPartitionReassignmentsOptions options) {
        return instance.listPartitionReassignments(partitions, options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(ListPartitionReassignmentsOptions options) {
        return instance.listPartitionReassignments(options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions, ListPartitionReassignmentsOptions options) {
        return instance.listPartitionReassignments(partitions, options);
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options) {
        return instance.removeMembersFromConsumerGroup(groupId, options);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        return instance.alterConsumerGroupOffsets(groupId, offsets);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                     AlterConsumerGroupOffsetsOptions options) {
        return instance.alterConsumerGroupOffsets(groupId, offsets, options);
    }

    @Override
    public AlterStreamsGroupOffsetsResult alterStreamsGroupOffsets(String s, Map<TopicPartition, OffsetAndMetadata> map,
                                                                   AlterStreamsGroupOffsetsOptions alterStreamsGroupOffsetsOptions) {
        return instance.alterStreamsGroupOffsets(s, map, alterStreamsGroupOffsetsOptions);
    }

    @Override
    public AlterStreamsGroupOffsetsResult alterStreamsGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        return instance.alterStreamsGroupOffsets(groupId, offsets);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        return instance.listOffsets(topicPartitionOffsets);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        return instance.listOffsets(topicPartitionOffsets, options);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter) {
        return instance.describeClientQuotas(filter);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        return instance.describeClientQuotas(filter, options);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries) {
        return instance.alterClientQuotas(entries);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        return instance.alterClientQuotas(entries, options);
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials() {
        return instance.describeUserScramCredentials();
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users) {
        return instance.describeUserScramCredentials(users);
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        return instance.describeUserScramCredentials(users, options);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations) {
        return instance.alterUserScramCredentials(alterations);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
        return instance.alterUserScramCredentials(alterations, options);
    }

    @Override
    public DescribeFeaturesResult describeFeatures() {
        return instance.describeFeatures();
    }

    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
        return instance.describeFeatures(options);
    }

    @Override
    public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
        return instance.updateFeatures(featureUpdates, options);
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum() {
        return instance.describeMetadataQuorum();
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        return instance.describeMetadataQuorum(options);
    }

    @Override
    @InterfaceStability.Unstable
    public UnregisterBrokerResult unregisterBroker(int brokerId) {
        return instance.unregisterBroker(brokerId);
    }

    @Override
    @InterfaceStability.Unstable
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        return instance.unregisterBroker(brokerId, options);
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions) {
        return instance.describeProducers(partitions);
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options) {
        return instance.describeProducers(partitions, options);
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds) {
        return instance.describeTransactions(transactionalIds);
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        return instance.describeTransactions(transactionalIds, options);
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec) {
        return instance.abortTransaction(spec);
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        return instance.abortTransaction(spec, options);
    }

    @Override
    public ListTransactionsResult listTransactions() {
        return instance.listTransactions();
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        return instance.listTransactions(options);
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds) {
        return instance.fenceProducers(transactionalIds);
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        return instance.fenceProducers(transactionalIds, options);
    }

    @Override
    public ListConfigResourcesResult listConfigResources(Set<ConfigResource.Type> set, ListConfigResourcesOptions listConfigResourcesOptions) {
        return instance.listConfigResources(set, listConfigResourcesOptions);
    }

    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options) {
        return instance.listClientMetricsResources(options);
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return instance.clientInstanceId(timeout);
    }

    @Override
    public AddRaftVoterResult addRaftVoter(int i, Uuid uuid, Set<RaftVoterEndpoint> set, AddRaftVoterOptions addRaftVoterOptions) {
        return instance.addRaftVoter(i, uuid, set, addRaftVoterOptions);
    }

    @Override
    public RemoveRaftVoterResult removeRaftVoter(int i, Uuid uuid, RemoveRaftVoterOptions removeRaftVoterOptions) {
        return instance.removeRaftVoter(i, uuid, removeRaftVoterOptions);
    }

    @Override
    public DescribeShareGroupsResult describeShareGroups(Collection<String> groupIds, DescribeShareGroupsOptions options) {
        return instance.describeShareGroups(groupIds, options);
    }

    @Override
    public AlterShareGroupOffsetsResult alterShareGroupOffsets(String s, Map<TopicPartition, Long> map, AlterShareGroupOffsetsOptions alterShareGroupOffsetsOptions) {
        return instance.alterShareGroupOffsets(s, map, alterShareGroupOffsetsOptions);
    }

    @Override
    public ListShareGroupOffsetsResult listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec> map, ListShareGroupOffsetsOptions listShareGroupOffsetsOptions) {
        return instance.listShareGroupOffsets(map, listShareGroupOffsetsOptions);
    }

    @Override
    public DeleteShareGroupOffsetsResult deleteShareGroupOffsets(String s, Set<String> set, DeleteShareGroupOffsetsOptions deleteShareGroupOffsetsOptions) {
        return instance.deleteShareGroupOffsets(s, set, deleteShareGroupOffsetsOptions);
    }

    @Override
    public DeleteShareGroupsResult deleteShareGroups(Collection<String> collection, DeleteShareGroupsOptions deleteShareGroupsOptions) {
        return instance.deleteShareGroups(collection, deleteShareGroupsOptions);
    }

    @Override
    public DescribeStreamsGroupsResult describeStreamsGroups(Collection<String> collection, DescribeStreamsGroupsOptions describeStreamsGroupsOptions) {
        return instance.describeStreamsGroups(collection, describeStreamsGroupsOptions);
    }

    @Override
    public DescribeClassicGroupsResult describeClassicGroups(Collection<String> groupIds, DescribeClassicGroupsOptions options) {
        return instance.describeClassicGroups(groupIds, options);
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
    public Map<MetricName, ? extends Metric> metrics() {
        return instance.metrics();
    }

    @Override
    public TerminateTransactionResult forceTerminateTransaction(String s, TerminateTransactionOptions terminateTransactionOptions) {
        return instance.forceTerminateTransaction(s, terminateTransactionOptions);
    }

    @Override
    public ListConfigResourcesResult listConfigResources() {
        return instance.listConfigResources();
    }

    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources() {
        return instance.listClientMetricsResources();
    }

    @Override
    public AddRaftVoterResult addRaftVoter(int voterId, Uuid voterDirectoryId, Set<RaftVoterEndpoint> endpoints) {
        return instance.addRaftVoter(voterId, voterDirectoryId, endpoints);
    }

    @Override
    public RemoveRaftVoterResult removeRaftVoter(int voterId, Uuid voterDirectoryId) {
        return instance.removeRaftVoter(voterId, voterDirectoryId);
    }

    @Override
    public DescribeShareGroupsResult describeShareGroups(Collection<String> groupIds) {
        return instance.describeShareGroups(groupIds);
    }

    @Override
    public AlterShareGroupOffsetsResult alterShareGroupOffsets(String groupId, Map<TopicPartition, Long> offsets) {
        return instance.alterShareGroupOffsets(groupId, offsets);
    }

    @Override
    public ListShareGroupOffsetsResult listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec> groupSpecs) {
        return instance.listShareGroupOffsets(groupSpecs);
    }

    @Override
    public DeleteShareGroupOffsetsResult deleteShareGroupOffsets(String groupId, Set<String> topics) {
        return instance.deleteShareGroupOffsets(groupId, topics);
    }

    @Override
    public DeleteShareGroupsResult deleteShareGroups(Collection<String> groupIds) {
        return instance.deleteShareGroups(groupIds);
    }

    @Override
    public DescribeClassicGroupsResult describeClassicGroups(Collection<String> groupIds) {
        return instance.describeClassicGroups(groupIds);
    }

    @Override
    public DescribeStreamsGroupsResult describeStreamsGroups(Collection<String> groupIds) {
        return instance.describeStreamsGroups(groupIds);
    }

    @Override
    public TerminateTransactionResult forceTerminateTransaction(String transactionalId) {
        return instance.forceTerminateTransaction(transactionalId);
    }
}
