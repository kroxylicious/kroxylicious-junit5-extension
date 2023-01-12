/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import static java.util.Arrays.asList;

public class KafkaClusterTestInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext extensionContext) {
        Method testTemplateMethod = extensionContext.getRequiredTestMethod();
        Parameter[] parameters = testTemplateMethod.getParameters();

        Parameter parameter = Arrays.stream(parameters).filter(p -> KafkaClusterTestCase.class.isAssignableFrom(p.getType())).findFirst().get();

        Version version = Arrays.stream(parameter.getAnnotationsByType(Version.class)).findFirst().orElse(null);
        String kafkaVersion = version != null ? version.value() : "latest";

        boolean kraftModeOn = true;
        boolean zookeeperModeOn = false;

        List<TestTemplateInvocationContext> invocationContexts = new ArrayList<>();

        invocationContexts.add(kafkaClusterContext(
                new KafkaClusterTestCase(
                        "Kraft mode for version " + kafkaVersion,
                        2,
                        kraftModeOn,
                        kafkaVersion)));
        invocationContexts.add(kafkaClusterContext(
                new KafkaClusterTestCase(
                        "Zookeeper mode for version " + kafkaVersion,
                        2,
                        zookeeperModeOn,
                        kafkaVersion)));

        return invocationContexts.stream();
    }

    private TestTemplateInvocationContext kafkaClusterContext(KafkaClusterTestCase kafkaClusterTestCase) {
        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return kafkaClusterTestCase.getDisplayName();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return asList(
                        new GenericTypedParameterResolver<>(kafkaClusterTestCase),
                        (BeforeTestExecutionCallback) extensionContext -> System.out.println("BeforeTestExecutionCallback"),
                        (AfterTestExecutionCallback) extensionContext -> System.out.println("AfterTestExecutionCallback"));
            }
        };
    }
}