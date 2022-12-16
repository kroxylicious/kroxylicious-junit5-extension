package io.kroxylicious.testing.kafka.common;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        List<String> kafkaVersions = version != null ? asList(version.values()) : asList("latest");

        boolean kraftModeOn = true;
        boolean zookeeperModeOn = false;

        List<TestTemplateInvocationContext> invocationContexts = new ArrayList<>();

        for(String kafkaVersion : kafkaVersions) {
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
        }

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
                        (AfterTestExecutionCallback) extensionContext -> System.out.println("AfterTestExecutionCallback")
                );
            }
        };
    }
}
