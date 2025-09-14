/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.util.stream.Stream;

import org.apache.kafka.common.internals.Topic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension.TopicNaming;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension.TopicNaming.RandomHumanReadable;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension.TopicNaming.ReflectiveFactoryMethod;

import static io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtensionTest.Another.ANOTHER_TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
class KafkaClusterExtensionTest {

    public static final String STATIC_TOPIC_NAME = "topicName";
    public static final String INSTANCE_TOPIC_NAME = "instance";
    @Mock
    ExtensionContext extensionContext;

    public static Stream<Arguments> testClassAndDefault() {
        return Stream.of(Arguments.argumentSet("explicit", KafkaClusterExtensionTest.class), Arguments.argumentSet("default", Void.class));
    }

    @Test
    void randomHumanReadableName() {
        RandomHumanReadable randomHumanReadable = new RandomHumanReadable();
        String name = randomHumanReadable.name();
        assertThat(name).isNotNull();
        assertThatCode(() -> Topic.validate(name)).doesNotThrowAnyException();
    }

    @MethodSource("testClassAndDefault")
    @ParameterizedTest
    void staticMethodOnTestClassWithDefaultClass(Class<?> testClass) {
        doReturn(this.getClass()).when(extensionContext).getRequiredTestClass();
        TopicNaming topicNaming = new ReflectiveFactoryMethod(extensionContext, testClass, "staticTopicName");
        assertThat(topicNaming.name()).isEqualTo(STATIC_TOPIC_NAME);
    }

    @Test
    void staticMethodOnNonTestClass() {
        doReturn(this.getClass()).when(extensionContext).getRequiredTestClass();
        TopicNaming topicNaming = new ReflectiveFactoryMethod(extensionContext, Another.class, "topicName");
        assertThat(topicNaming.name()).isEqualTo(ANOTHER_TOPIC_NAME);
    }

    @MethodSource("testClassAndDefault")
    @ParameterizedTest
    void nullReturnNotAllowed(Class<?> testClass) {
        doReturn(this.getClass()).when(extensionContext).getRequiredTestClass();
        TopicNaming topicNaming = new ReflectiveFactoryMethod(extensionContext, testClass, "invalidMethod_nullReturn");
        assertThatThrownBy(topicNaming::name).isInstanceOf(ParameterResolutionException.class)
                .hasMessage(
                        "topic name source method static java.lang.String io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtensionTest.invalidMethod_nullReturn() returned null");
    }

    @MethodSource("testClassAndDefault")
    @ParameterizedTest
    void methodMustHaveZeroParameters(Class<?> testClass) {
        doReturn(this.getClass()).when(extensionContext).getRequiredTestClass();
        TopicNaming topicNaming = new ReflectiveFactoryMethod(extensionContext, testClass, "invalidMethod_hasParameter");
        assertThatThrownBy(topicNaming::name).isInstanceOf(ParameterResolutionException.class)
                .hasMessage("failed to invoke topic source name method invalidMethod_hasParameter of io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtensionTest");
    }

    @MethodSource("testClassAndDefault")
    @ParameterizedTest
    void methodMustReturnString(Class<?> testClass) {
        doReturn(this.getClass()).when(extensionContext).getRequiredTestClass();
        TopicNaming topicNaming = new ReflectiveFactoryMethod(extensionContext, testClass, "invalidMethod_notStringReturnType");
        assertThatThrownBy(topicNaming::name).isInstanceOf(ParameterResolutionException.class)
                .hasMessage(
                        "topic name source method private static int io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtensionTest.invalidMethod_notStringReturnType() must return String");
    }

    @Test
    void nonStaticMethodDisallowed() {
        doReturn(this.getClass()).when(extensionContext).getRequiredTestClass();
        TopicNaming topicNaming = new ReflectiveFactoryMethod(extensionContext, Another.class, "instanceMethod");
        assertThatThrownBy(topicNaming::name).isInstanceOf(ParameterResolutionException.class)
                .hasMessage(
                        "topic name source method java.lang.String io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtensionTest$Another.instanceMethod() is not static.");
    }

    public static Stream<Arguments> reflectiveFactoryMethodConstructorArgsMustBeNonNull() {
        ExtensionContext context = Mockito.mock(ExtensionContext.class);
        return Stream.of(Arguments.argumentSet("null context", (Runnable) () -> {
            new ReflectiveFactoryMethod(null, Void.class, "method");
        }, "context must not be null"),
                Arguments.argumentSet("null methodClass", (Runnable) () -> {
                    new ReflectiveFactoryMethod(context, null, "method");
                }, "methodClass must not be null"),
                Arguments.argumentSet("null method", (Runnable) () -> {
                    new ReflectiveFactoryMethod(context, Void.class, null);
                }, "method must not be null"));
    }

    @MethodSource
    @ParameterizedTest
    void reflectiveFactoryMethodConstructorArgsMustBeNonNull(Runnable runnable, String errorMessage) {
        assertThatThrownBy(runnable::run).isInstanceOf(NullPointerException.class).hasMessage(errorMessage);
    }

    @SuppressWarnings("unused") // used reflectively
    static String staticTopicName() {
        return STATIC_TOPIC_NAME;
    }

    @SuppressWarnings("unused") // used reflectively
    static String invalidMethod_nullReturn() {
        return null;
    }

    @SuppressWarnings("unused") // used reflectively
    private static int invalidMethod_notStringReturnType() {
        throw new IllegalStateException("should never be invoked");
    }

    @SuppressWarnings("unused") // used reflectively
    static String invalidMethod_hasParameter(int ignored) {
        throw new IllegalStateException("should never be invoked");
    }

    @SuppressWarnings("unused")
    // used reflectively
    String instanceTopicName() {
        throw new IllegalStateException("should never be invoked");
    }

    public record Another() {

        public static final String ANOTHER_TOPIC_NAME = "another";

        @SuppressWarnings("unused") // used reflectively
        static String topicName() {
            return ANOTHER_TOPIC_NAME;
        }

        @SuppressWarnings("unused")
        // used reflectively
        String instanceMethod() {
            throw new IllegalStateException("should never be invoked");
        }
    }

}
