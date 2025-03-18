/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.invm;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReflectionUtilsTest {

    @Test
    void constructNoArgs() {
        var instance = ReflectionUtils.construct(NoArgs.class);
        assertThat(instance)
                .isPresent()
                .get()
                .isInstanceOf(NoArgs.class);
    }

    @Test
    void constructWithObjectArgs() {
        var instance = ReflectionUtils.construct(ObjectArg.class, "foo");
        assertThat(instance)
                .isPresent()
                .get()
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectArg.class))
                .extracting(ObjectArg::getArg)
                .isEqualTo("foo");
    }

    @Test
    void constructWithPrimitiveArgs() {
        var instance = ReflectionUtils.construct(PrimitiveArg.class, true);
        assertThat(instance)
                .isPresent()
                .get()
                .asInstanceOf(InstanceOfAssertFactories.type(PrimitiveArg.class))
                .extracting(PrimitiveArg::getArg)
                .isEqualTo(true);
    }

    @Test
    void noMatchingConstructor() {
        var instance = ReflectionUtils.construct(ObjectArg.class, 12);
        assertThat(instance).isEmpty();
    }

    @Test
    void invokeInstanceMethodWithNoArgs() {
        var obj = new ObjectArg("foo");

        var result = ReflectionUtils.invokeInstanceMethod(obj, "getArg");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void invokeInstanceMethodWithArgsReturningNonVoid() {
        var obj = new MyObject();

        var result = ReflectionUtils.invokeInstanceMethod(obj, "reverseMe", "oof");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void invokeInstanceMethodWithArgsReturningVoid() {
        var obj = new MyObject();

        var result = ReflectionUtils.invokeInstanceMethod(obj, "myMethodReturningVoid", "oof");
        assertThat(result).isNull();
    }

    @Test
    void invokeStaticMethodWithArgsReturningNonVoid() {
        var result = ReflectionUtils.invokeStaticMethod(MyObject.class, "staticReverseMe", "oof");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void invokeInstanceMethodNotFound() {
        var obj = new MyObject();

        assertThatThrownBy(() -> ReflectionUtils.invokeInstanceMethod(obj, "notFound"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Can't find method notFound on class io.kroxylicious.testing.kafka.invm.ReflectionUtilsTest$MyObject");
    }

    @Test
    void invokeStaticMethodNotFound() {
        assertThatThrownBy(() -> ReflectionUtils.invokeStaticMethod(MyObject.class, "notFound"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Can't find method notFound on class io.kroxylicious.testing.kafka.invm.ReflectionUtilsTest$MyObject");
    }

    @Test
    void detectsInstanceMethodInvokedStatic() {
        assertThatThrownBy(() -> ReflectionUtils.invokeStaticMethod(MyObject.class, "reverseMe"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Can't find method reverseMe on class io.kroxylicious.testing.kafka.invm.ReflectionUtilsTest$MyObject");
    }

    // These classes are used reflectively by the tests.
    static class NoArgs {
        public NoArgs() {
            super();
        }
    }

    static class ObjectArg {
        private final String arg;

        public ObjectArg(String arg) {
            super();
            this.arg = arg;
        }

        public String getArg() {
            return arg;
        }
    }

    static class PrimitiveArg {
        private final boolean arg;

        public PrimitiveArg(boolean arg) {
            super();
            this.arg = arg;
        }

        public boolean getArg() {
            return arg;
        }
    }

    static class MyObject {

        public String reverseMe(String arg) {
            return new StringBuilder(arg).reverse().toString();
        }

        public void myMethodReturningVoid(String arg) {
        }

        public static String staticReverseMe(String arg) {
            return new StringBuilder(arg).reverse().toString();
        }
    }
}