/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.testkit.engine.Events;

import io.kroxylicious.cluster.KafkaCluster;
import io.kroxylicious.junit5.constraint.BrokerCluster;
import io.kroxylicious.junit5.constraint.BrokerConfig;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.testkit.engine.EngineTestKit.engine;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.EventConditions.test;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

public class ExceptionalTest {
    @ExtendWith(KafkaClusterExtension.class)
    static class ImpossibleConstraintCase {

        @BrokerCluster(numBrokers = 1)
        @ImpossibleConstraint
        KafkaCluster cluster;

        // Test that an unknown @KafkaClusterConstraint-annotated annotation
        // is an impossible-to-satisfy constraint
        @Test
        public void impossibleConstraint() {
            fail("Test execution shouldn't get this far");
        }
    }

    @Test
    void verifyImpossibleConstraintResultsInException() {
        String methodName = "impossibleConstraint";

        Events impossibleConstraint = engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(ImpossibleConstraintCase.class))
                .execute()
                .allEvents();
        impossibleConstraint.assertStatistics(s -> s.failed(1));
        impossibleConstraint
                .assertThatEvents().haveExactly(1,
                        event(test(methodName),
                                finishedWithFailure(
                                        instanceOf(ExtensionConfigurationException.class),
                                        message("No provisioning strategy for a declaration of " +
                                                "type io.kroxylicious.cluster.KafkaCluster and supporting all " +
                                                "of [@io.kroxylicious.junit5.constraint.BrokerCluster(numBrokers=1), @io.kroxylicious.junit5.ImpossibleConstraint()] was "
                                                +
                                                "found (tried: [io.kroxylicious.junit5.InVMProvisioningStrategy, " +
                                                "io.kroxylicious.junit5.TestcontainersProvisioningStrategy])"))));
    }

    @ExtendWith(KafkaClusterExtension.class)
    static class AmbiguousClusterCase {
        // Test that an unknown @KafkaClusterConstraint-annotated annotation
        // is an impossible-to-satisfy constraint
        @Test
        public void ambiguousCluster(@BrokerCluster(numBrokers = 1) KafkaCluster cluster,
                                     @BrokerCluster(numBrokers = 1) KafkaCluster clustererberg,
                                     Admin ambiguousAdmin) {
            fail("Test execution shouldn't get this far");
        }
    }

    @Test
    void verifyAmbiguousClusterResultsInException() {
        String methodName = "ambiguousCluster";

        Events impossibleConstraint = engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(AmbiguousClusterCase.class))
                .execute()
                .allEvents();
        impossibleConstraint.assertStatistics(s -> s.failed(1));
        impossibleConstraint
                .assertThatEvents().haveExactly(1,
                        event(test(methodName),
                                finishedWithFailure(
                                        instanceOf(AmbiguousKafkaClusterException.class),
                                        message("KafkaCluster to associate with parameter " +
                                                "ambiguousAdmin is ambiguous, use @Name on the intended " +
                                                "cluster and this element to disambiguate"))));
    }

    @ExtendWith(KafkaClusterExtension.class)
    static class DuplicateNameCase {
        // throw if two clusters declared with same cluster id
        @Test
        public void duplicateName(
                                  @BrokerCluster(numBrokers = 1) @Name("A") KafkaCluster cluster1,
                                  @BrokerCluster(numBrokers = 2) @Name("A") KafkaCluster cluster2) {
            fail("Test execution shouldn't get this far");
        }
    }

    @Test
    void verifyDuplicateName() {
        String methodName = "duplicateName";

        Events impossibleConstraint = engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(DuplicateNameCase.class))
                .execute()
                .allEvents();
        impossibleConstraint.assertStatistics(s -> s.failed(1));
        impossibleConstraint
                .assertThatEvents().haveExactly(1,
                        event(test(methodName),
                                finishedWithFailure(
                                        instanceOf(ParameterResolutionException.class),
                                        message("Failed to resolve parameter " +
                                                "[io.kroxylicious.cluster.KafkaCluster cluster2] in method " +
                                                "[public void io.kroxylicious.junit5.ExceptionalTest$DuplicateNameCase.duplicateName(io.kroxylicious.cluster.KafkaCluster,io.kroxylicious.cluster.KafkaCluster)]: "
                                                +
                                                "A KafkaCluster-typed declaration with @Name(\"A\") is already in scope"))));
    }

    @ExtendWith(KafkaClusterExtension.class)
    static class ProhibitedBrokerConfigCase {

        // Test that the test author cannot mistakenly think they
        // overrode a broker config used by the extension itself
        @Test
        public void prohibitedBrokerConfig(@BrokerConfig(name = "broker.id", value = "0") KafkaCluster cluster) {
            fail("Test execution shouldn't get this far");
        }
    }

    @Test
    void verifyProhibitedBrokerConfig() {
        String methodName = "prohibitedBrokerConfig";

        Events impossibleConstraint = engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(ProhibitedBrokerConfigCase.class))
                .execute()
                .allEvents();
        impossibleConstraint.assertStatistics(s -> s.failed(1));
        impossibleConstraint
                .assertThatEvents().haveExactly(1,
                        event(test(methodName),
                                finishedWithFailure(
                                        instanceOf(ParameterResolutionException.class),
                                        message("Failed to resolve parameter " +
                                                "[io.kroxylicious.cluster.KafkaCluster cluster] " +
                                                "in method " +
                                                "[public void io.kroxylicious.junit5.ExceptionalTest$ProhibitedBrokerConfigCase.prohibitedBrokerConfig(io.kroxylicious.cluster.KafkaCluster)]: "
                                                +
                                                "Cannot override broker config 'broker.id=0' with new value 0"))));
    }
}
