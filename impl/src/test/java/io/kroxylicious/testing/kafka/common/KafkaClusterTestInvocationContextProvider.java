package io.kroxylicious.testing.kafka.common;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

public class KafkaClusterTestInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext extensionContext) {
        boolean kraftModeOn = true;
        boolean zookeeperModeOn = false;

        return Stream.of(
                kafkaClusterContext(
                        new KafkaClusterTestCase(
                                "Kraft mode for version 3.3.1",
                                2,
                                kraftModeOn,
                                "3.3.1")),
                kafkaClusterContext(
                        new KafkaClusterTestCase(
                                "Zookeeper mode for version 3.3.1",
                                2,
                                zookeeperModeOn,
                                "3.3.1"))
        );
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
                        new GenericTypedParameterResolver(kafkaClusterTestCase),
                        (BeforeTestExecutionCallback) extensionContext -> System.out.println("BeforeTestExecutionCallback"),
                        (AfterTestExecutionCallback) extensionContext -> System.out.println("AfterTestExecutionCallback")
                );
            }
        };
    }
}
