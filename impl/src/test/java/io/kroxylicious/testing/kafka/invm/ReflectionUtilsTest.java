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
    void constructWithNoParameters() {
        var instance = ReflectionUtils.construct(NoArgs.class);
        assertThat(instance)
                .isPresent()
                .get()
                .isInstanceOf(NoArgs.class);
    }

    @Test
    void constructWithOneParameter() {
        var instance = ReflectionUtils.construct(ObjectArg.class, "foo");
        assertThat(instance)
                .isPresent()
                .get()
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectArg.class))
                .extracting(ObjectArg::getArg)
                .isEqualTo("foo");
    }

    @Test
    void constructWithPrimitiveParameter() {
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
    void constructWithParameterWithNullValueRejected() {
        assertThatThrownBy(() -> ReflectionUtils.construct(ObjectArg.class, "foo", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Null parameters are not supported (parameter 2 was null).");
    }

    @Test
    void constructorThrowsCheckedException() {
        var myException = new Exception("my checked exception");
        assertThatThrownBy(() -> ReflectionUtils.construct(ThrowingObj.class, myException))
                .rootCause()
                .isEqualTo(myException);
    }

    @Test
    void constructorThrowsUncheckedException() {
        var myException = new RuntimeException("my unchecked exception");
        assertThatThrownBy(() -> ReflectionUtils.construct(ThrowingObj.class, myException))
                .rootCause()
                .isEqualTo(myException);
    }

    @Test
    void invokeInstanceMethodWithNoParameters() {
        var obj = new ObjectArg("foo");

        var result = ReflectionUtils.invokeInstanceMethod(obj, "getArg");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void invokeInstanceMethodWithParameterReturningNonVoid() {
        var obj = new MyObject();

        var result = ReflectionUtils.invokeInstanceMethod(obj, "reverseMe", "oof");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void invokeInstanceMethodWithParameterReturningVoid() {
        var obj = new MyObject();

        var result = ReflectionUtils.invokeInstanceMethod(obj, "myMethodReturningVoid", "oof");
        assertThat(result).isNull();
    }

    @Test
    void invokeOverloadedInstanceMethodWithParameters() {
        var obj = new ObjectWithOverloadedMethod();

        var result = ReflectionUtils.invokeInstanceMethod(obj, "myOverloadedMethod", "arg");
        assertThat(result).isEqualTo("1arg");
    }

    @Test
    void invokeOverloadedInstanceMethodWithoutParameters() {
        var obj = new ObjectWithOverloadedMethod();

        var result = ReflectionUtils.invokeInstanceMethod(obj, "myOverloadedMethod");
        assertThat(result).isEqualTo("0arg");
    }

    @Test
    void invokeStaticMethodWithParameterReturningNonVoid() {
        var result = ReflectionUtils.invokeStaticMethod(MyObject.class, "staticReverseMe", "oof");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void instanceMethodNotFound() {
        var obj = new MyObject();

        assertThatThrownBy(() -> ReflectionUtils.invokeInstanceMethod(obj, "notFound"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Can't find method notFound on class io.kroxylicious.testing.kafka.invm.ReflectionUtilsTest$MyObject");
    }

    @Test
    void staticMethodNotFound() {
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

    @Test
    void invokeWithParameterWithNullValueRejected() {
        var obj = new MyObject();

        assertThatThrownBy(() -> ReflectionUtils.invokeInstanceMethod(obj, "myMethodReturningVoid", null, "foo"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Null parameters are not supported (parameter 1 was null).");
    }

    @Test
    void invokeThrowsCheckedException() {
        var thrower = new ThrowingObj();
        var myException = new Exception("my checked exception");

        assertThatThrownBy(() -> ReflectionUtils.invokeInstanceMethod(thrower, "thrower", myException))
                .rootCause()
                .isEqualTo(myException);
    }

    @Test
    void invokeThrowsUncheckedException() {
        var thrower = new ThrowingObj();
        var myException = new RuntimeException("my unchecked exception");
        assertThatThrownBy(() -> ReflectionUtils.invokeInstanceMethod(thrower, "thrower", myException))
                .rootCause()
                .isEqualTo(myException);
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

    static class ObjectWithOverloadedMethod {

        public String myOverloadedMethod(String arg) {
            return "1arg";
        }

        public String myOverloadedMethod() {
            return "0arg";
        }

        public static String staticReverseMe(String arg) {
            return new StringBuilder(arg).reverse().toString();
        }
    }

    static class ThrowingObj {
        public ThrowingObj() {
        }

        public ThrowingObj(Exception e) throws Exception {
            throw e;
        }

        public void thrower(Exception e) throws Exception {
            throw e;
        }
    }

}