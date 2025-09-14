/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link TopicNameMethodSource} is used to customize topic naming. It may be applied to the {@link Topic} type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
public @interface TopicNameMethodSource {

    /**
     * The name of the {@code static}, package- or {@code public}-accessible method.
     * Defaults to 'topicName'.
     * @return the string
     */
    String value() default "topicName";

    /**
     * The class where defining the static method, or Void.class (default), if the method
     * is defined with the class defining the annotation test.
     * @return the class
     */
    Class<?> clazz() default Void.class;

}
