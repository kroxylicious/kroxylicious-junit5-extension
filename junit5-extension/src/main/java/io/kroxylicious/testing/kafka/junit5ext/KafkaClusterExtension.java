/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.junit5ext;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.ListDeserializer;
import org.apache.kafka.common.serialization.ListSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.KafkaClusterConstraint;
import io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy;

import static java.lang.System.Logger.Level.TRACE;
import static org.junit.platform.commons.support.ReflectionSupport.findFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

/**
 * A JUnit 5 extension that allows declarative injection of a {@link KafkaCluster} into a test
 * via static or instance field(s) and/or parameters.
 *
 * <p>A simple example looks like:</p>
 * <pre>{@code
 * import io.kroxylicious.junit5.KafkaClusterExtension;
 * import org.apache.kafka.clients.producer.Producer;
 * import org.apache.kafka.clients.producer.ProducerRecord;
 * import org.junit.jupiter.api.Test;
 * import org.junit.jupiter.api.extension.ExtendWith;
 *
 * @ExtendWith(KafkaClusterExtension.class) // <1>
 * class MyTest {
 *
 *     KafkaCluster cluster; // <2>
 *
 *     @Test
 *     public void testProducer(
 *                 Producer<String, String> producer // <3>
 *             ) throws Exception {
 *         producer.send(new ProducerRecord<>("hello", "world")).get();
 *     }
 * }
 * }</pre>
 *
 * <p>Notes:</p>
 * <ol>
 * <li>You have to tell Junit that you're using the extension using {@code @ExtendWith}.</li>
 * <li>An instance field of type {@link KafkaCluster} will cause a new cluster to be provisioned for
 * each test in the class. Alternatively you can use a parameter on a
 * {@code @Test}-annotated method. If you use a {@code static} field then a single
 * cluster will be provisioned for all the tests in the class.</li>
 * <li>Your test methods can declare {@code Producer}, {@code Consumer} and {@code Admin}-typed parameters.
 * They will be configured to bootstrap against the {@code cluster}.</li>
 * </ol>
 */
public class KafkaClusterExtension implements
        ParameterResolver, BeforeEachCallback,
        BeforeAllCallback, TestTemplateInvocationContextProvider {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterExtension.class.getName());

    private static final ExtensionContext.Namespace CLUSTER_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, KafkaCluster.class);
    private static final ExtensionContext.Namespace ADMIN_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, Admin.class);
    private static final ExtensionContext.Namespace PRODUCER_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, Producer.class);
    private static final ExtensionContext.Namespace CONSUMER_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, Consumer.class);
    public static final String STARTING_PREFIX = "WY9Br5K1vAfov_8jjJ3KUA";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        Parameter[] parameters = context.getRequiredTestMethod().getParameters();
        for (var parameter : parameters) {
            if (!supportsParameter(parameter)) {
                return false;
            }
        }
        return true;
    }

    private static List<? extends List<? extends Object>> cartesianProduct(List<List<?>> domains) {
        if (domains.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return _cartesianProduct(0, domains);
    }

    private static List<? extends List<? extends Object>> _cartesianProduct(int index, List<List<?>> domains) {
        List<List<Object>> ret = new ArrayList<>();
        if (index == domains.size()) {
            ret.add(new ArrayList<>(domains.size()));
        }
        else {
            for (Object obj : domains.get(index)) {
                for (List tuple : _cartesianProduct(index + 1, domains)) {
                    tuple.add(0, obj);
                    ret.add(tuple);
                }
            }
        }
        return ret;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        Method testTemplateMethod = context.getRequiredTestMethod();
        Parameter[] parameters = testTemplateMethod.getParameters();

        Parameter parameter = Arrays.stream(parameters).filter(p -> KafkaCluster.class.isAssignableFrom(p.getType())).findFirst().get();
        DimensionMethodSource[] freeConstraintsSource = parameter.getAnnotationsByType(DimensionMethodSource.class);

        var lists = Arrays.stream(freeConstraintsSource).map(methodSource -> {
            return invokeDimensionMethodSource(context, methodSource);
        }).collect(Collectors.toList());
        List<? extends List<Annotation>> cartesianProduct = lists.size() > 0 ? cartesianProduct((List) lists) : List.of();

        ConstraintsMethodSource annotation = parameter.getAnnotation(ConstraintsMethodSource.class);
        var constraints = annotation != null ? invokeConstraintsMethodSource(context, annotation) : List.<List<Annotation>> of();

        return Stream.concat(cartesianProduct.stream(), constraints.stream())
                .map((List<Annotation> additionalConstraints) -> {
                    return new TestTemplateInvocationContext() {
                        @Override
                        public String getDisplayName(int invocationIndex) {
                            List<?> list = invocationIndex > cartesianProduct.size() ? constraints.get(invocationIndex - cartesianProduct.size() - 1)
                                    : cartesianProduct.get(invocationIndex - 1);
                            return list.toString();
                        }

                        @Override
                        public List<Extension> getAdditionalExtensions() {
                            return List.of(new ParameterResolver() {
                                @Override
                                public boolean supportsParameter(ParameterContext parameterContext,
                                                                 ExtensionContext extensionContext) {
                                    return KafkaClusterExtension.supportsParameter(parameterContext.getParameter());
                                }

                                @Override
                                public Object resolveParameter(ParameterContext parameterContext,
                                                               ExtensionContext extensionContext) {
                                    return KafkaClusterExtension.resolveParameter(parameterContext, extensionContext, additionalConstraints);
                                }
                            });
                        }
                    };
                });
    }

    @NotNull
    private static List<List<Annotation>> invokeConstraintsMethodSource(ExtensionContext context,
                                                                        ConstraintsMethodSource methodSource) {
        Method testTemplateMethod = context.getRequiredTestMethod();
        Class<?> requiredTestClass = context.getRequiredTestClass();
        Object source;
        try {
            Method sourceMethod = requiredTestClass.getDeclaredMethod(methodSource.value());
            if (ReflectionUtils.isNotStatic(sourceMethod)) {
                throw new ParameterResolutionException("Method " + methodSource.value() + " given in @" + ConstraintsMethodSource.class.getSimpleName() +
                        " on " + requiredTestClass + " must be static");
            }
            else if (sourceMethod.getParameters().length != 0) {
                throw new ParameterResolutionException("Method " + methodSource.value() + " given in @" + ConstraintsMethodSource.class.getSimpleName() +
                        " on " + requiredTestClass + " cannot have any parameters");
            }
            Class<?> returnType = sourceMethod.getReturnType();
            // check return type is Stream<? extends Annotation>
            if (Stream.class.isAssignableFrom(returnType)) {
                Type genericReturnType = sourceMethod.getGenericReturnType();
                if (genericReturnType instanceof ParameterizedType) {
                    if (Stream.class.equals(((ParameterizedType) genericReturnType).getRawType())
                            && ((ParameterizedType) genericReturnType).getActualTypeArguments()[0] instanceof Class
                            && !((Class) ((ParameterizedType) genericReturnType).getActualTypeArguments()[0]).isAnnotation()) {
                        throw returnTypeError(testTemplateMethod, methodSource.value(), ConstraintsMethodSource.class, requiredTestClass);
                    }
                }
            }
            else if (Collection.class.isAssignableFrom(returnType)) {
                Type genericReturnType = sourceMethod.getGenericReturnType();
                if (genericReturnType instanceof ParameterizedType) {
                    if (Collection.class.equals(((ParameterizedType) genericReturnType).getRawType())
                            && ((ParameterizedType) genericReturnType).getActualTypeArguments()[0] instanceof Class
                            && !((Class) ((ParameterizedType) genericReturnType).getActualTypeArguments()[0]).isAnnotation()) {
                        throw returnTypeError(testTemplateMethod, methodSource.value(), ConstraintsMethodSource.class, requiredTestClass);
                    }
                }
            }
            else if (returnType.isArray()) {
                var elementType = returnType.getComponentType();
                if (!elementType.isAnnotation()) {
                    throw returnTypeError(testTemplateMethod, methodSource.value(), ConstraintsMethodSource.class, requiredTestClass);
                }
            }
            else {
                throw new ParameterResolutionException("Method " + methodSource.value() + " given in @" + DimensionMethodSource.class.getSimpleName() +
                        " on " + requiredTestClass + " must return a Stream, a Collection, or an array with" +
                        "Annotation type");
            }

            // TODO check that annotation is meta-annotated
            source = ReflectionUtils.makeAccessible(sourceMethod).invoke(null);
        }
        catch (ReflectiveOperationException e) {
            throw new ParameterResolutionException("Error invoking method " + methodSource.value() + " given in @" + DimensionMethodSource.class.getSimpleName() +
                    " on " + requiredTestClass, e);
        }

        return KafkaClusterExtension.<List<Annotation>> coerceToList(
                methodSource.value(), ConstraintsMethodSource.class,
                testTemplateMethod, requiredTestClass, source).stream()
                .map(list -> filterAnnotations(list, KafkaClusterConstraint.class))
                .collect(Collectors.toList());
    }

    @NotNull
    private static List<Annotation> invokeDimensionMethodSource(ExtensionContext context,
                                                                DimensionMethodSource methodSource) {
        Method testTemplateMethod = context.getRequiredTestMethod();
        Class<?> requiredTestClass = context.getRequiredTestClass();
        Object source;
        try {
            Method sourceMethod = ReflectionUtils.makeAccessible(requiredTestClass.getDeclaredMethod(methodSource.value()));
            if (ReflectionUtils.isNotStatic(sourceMethod)) {
                throw new ParameterResolutionException("Method " + methodSource.value() + " given in @" + DimensionMethodSource.class.getSimpleName() +
                        " on " + requiredTestClass + " must be static");
            }
            else if (sourceMethod.getParameters().length != 0) {
                throw new ParameterResolutionException("Method " + methodSource.value() + " given in @" + DimensionMethodSource.class.getSimpleName() +
                        " on " + requiredTestClass + " cannot have any parameters");
            }
            Class<?> returnType = sourceMethod.getReturnType();
            // check return type is Stream<? extends Annotation>
            if (Stream.class.isAssignableFrom(returnType)) {
                Type genericReturnType = sourceMethod.getGenericReturnType();
                if (genericReturnType instanceof ParameterizedType) {
                    if (Stream.class.equals(((ParameterizedType) genericReturnType).getRawType())
                            && ((ParameterizedType) genericReturnType).getActualTypeArguments()[0] instanceof Class
                            && !((Class) ((ParameterizedType) genericReturnType).getActualTypeArguments()[0]).isAnnotation()) {
                        throw returnTypeError(testTemplateMethod, methodSource.value(), DimensionMethodSource.class, requiredTestClass);
                    }
                }
            }
            else if (Collection.class.isAssignableFrom(returnType)) {
                Type genericReturnType = sourceMethod.getGenericReturnType();
                if (genericReturnType instanceof ParameterizedType) {
                    if (Collection.class.equals(((ParameterizedType) genericReturnType).getRawType())
                            && ((ParameterizedType) genericReturnType).getActualTypeArguments()[0] instanceof Class
                            && !((Class) ((ParameterizedType) genericReturnType).getActualTypeArguments()[0]).isAnnotation()) {
                        throw returnTypeError(testTemplateMethod, methodSource.value(), DimensionMethodSource.class, requiredTestClass);
                    }
                }
            }
            else if (returnType.isArray()) {
                var elementType = returnType.getComponentType();
                if (!elementType.isAnnotation()) {
                    throw returnTypeError(testTemplateMethod, methodSource.value(), DimensionMethodSource.class, requiredTestClass);
                }
            }
            else {
                throw new ParameterResolutionException("Method " + methodSource.value() + " given in @" + DimensionMethodSource.class.getSimpleName() +
                        " on " + requiredTestClass + " must return a Stream, a Collection, or an array with" +
                        "Annotation type");
            }

            source = sourceMethod.invoke(null);
        }
        catch (ReflectiveOperationException e) {
            throw new ParameterResolutionException("Error invoking method " + methodSource.value() + " given in @" + DimensionMethodSource.class.getSimpleName() +
                    " on " + requiredTestClass, e);
        }

        return filterAnnotations(coerceToList(
                methodSource.value(), DimensionMethodSource.class,
                testTemplateMethod, requiredTestClass, source), KafkaClusterConstraint.class);
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private static <T> List<T> coerceToList(String methodName,
                                            Class<? extends Annotation> annotationType,
                                            Method testTemplateMethod, Class<?> requiredTestClass, Object source) {
        List<T> list;
        if (source instanceof Stream) {
            list = ((Stream<T>) source).collect(Collectors.<T> toList());
        }
        else if (source instanceof List) {
            list = (List<T>) source;
        }
        else if (source instanceof Collection) {
            list = new ArrayList<>((Collection<T>) source);
        }
        else if (source instanceof Object[]) {
            list = Arrays.asList((T[]) source);
        }
        else {
            throw returnTypeError(testTemplateMethod, methodName, annotationType, requiredTestClass);
        }
        return list;
    }

    @NotNull
    private static ParameterResolutionException returnTypeError(Method testTemplateMethod,
                                                                String methodName,
                                                                Class<? extends Annotation> annotationType,
                                                                Class<?> requiredTestClass) {
        return new ParameterResolutionException("Method " + methodName + " given in @" + annotationType.getSimpleName() +
                " on " + testTemplateMethod.getName() + "() of " + requiredTestClass + " must return a Stream, a Collection, or an array with" +
                "Annotation type");
    }

    static class Closeable<T extends AutoCloseable> implements ExtensionContext.Store.CloseableResource {

        private final String clusterName;

        private final T resource;
        private final AnnotatedElement sourceElement;

        public Closeable(AnnotatedElement sourceElement, String clusterName, T resource) {
            this.sourceElement = sourceElement;
            this.clusterName = clusterName;
            this.resource = resource;
        }

        public T get() {
            return resource;
        }

        @Override
        public void close() throws Throwable {
            LOGGER.log(TRACE, "Stopping '{0}' with cluster name '{1}' for {2}",
                    resource, clusterName, sourceElement);
            resource.close();
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return !parameterContext.getDeclaringExecutable().isAnnotationPresent(TestTemplate.class)
                && supportsParameter(parameterContext.getParameter());
    }

    private static boolean supportsParameter(Parameter parameter) {
        Class<?> type = parameter.getType();
        if (KafkaCluster.class.isAssignableFrom(type)) {
            return true;
        }
        else if (Admin.class.isAssignableFrom(type)) {
            return true;
        }
        else if (Producer.class.isAssignableFrom(type)) {
            return true;
        }
        else if (Consumer.class.isAssignableFrom(type)) {
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public Object resolveParameter(
                                   ParameterContext parameterContext,
                                   ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return resolveParameter(parameterContext, extensionContext, List.of());
    }

    public static Object resolveParameter(
                                          ParameterContext parameterContext,
                                          ExtensionContext extensionContext,
                                          List<Annotation> extraConstraints)
            throws ParameterResolutionException {
        Parameter parameter = parameterContext.getParameter();
        Class<?> type = parameter.getType();
        LOGGER.log(TRACE,
                "test {0}: Resolving parameter ({1} {2})",
                extensionContext.getUniqueId(),
                type.getSimpleName(),
                parameter.getName());
        if (KafkaCluster.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(KafkaCluster.class);
            var constraints = getConstraintAnnotations(parameter, KafkaClusterConstraint.class);
            constraints.addAll(extraConstraints);
            return getCluster(parameter, paramType, constraints, extensionContext);
        }
        else if (Admin.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(Admin.class);
            return getAdmin("parameter " + parameter.getName(), parameter, paramType, extensionContext);
        }
        else if (Producer.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(Producer.class);
            Type paramGenericType = parameterContext.getDeclaringExecutable().getGenericParameterTypes()[parameterContext.getIndex()];
            return getProducer("parameter " + parameter.getName(), parameter, (Class) paramType, paramGenericType,
                    extensionContext);
        }
        else if (Consumer.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(Consumer.class);
            Type paramGenericType = parameterContext.getDeclaringExecutable().getGenericParameterTypes()[parameterContext.getIndex()];
            return getConsumer("parameter " + parameter.getName(), parameter, (Class) paramType, paramGenericType,
                    extensionContext);
        }
        else {
            throw new ExtensionConfigurationException("Could not resolve " + parameterContext);
        }
    }

    /**
     * Perform field injection for non-private, static fields
     * of type {@link KafkaCluster} or {@link KafkaCluster}.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        injectStaticFields(context, context.getRequiredTestClass());
    }

    /**
     * Perform field injection for non-private, instance fields
     * of type {@link KafkaCluster} or {@link KafkaCluster}.
     */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        context.getRequiredTestInstances().getAllInstances().forEach(instance -> injectInstanceFields(context, instance));
    }

    private void injectInstanceFields(ExtensionContext context, Object instance) {
        injectFields(context, instance, instance.getClass(), ReflectionUtils::isNotStatic);
    }

    private void injectStaticFields(ExtensionContext context, Class<?> testClass) {
        injectFields(context, null, testClass, ReflectionUtils::isStatic);
    }

    private void injectFields(ExtensionContext context, Object testInstance, Class<?> testClass, Predicate<Field> predicate) {
        findFields(
                testClass,
                field -> predicate.test(field) && KafkaCluster.class.isAssignableFrom(field.getType()),
                HierarchyTraversalMode.BOTTOM_UP)
                        .forEach(field -> {
                            assertSupportedType("field", field.getType());
                            try {
                                var accessibleField = makeAccessible(field);
                                List<Annotation> constraints = getConstraintAnnotations(accessibleField, KafkaClusterConstraint.class);
                                accessibleField.set(testInstance,
                                        getCluster(accessibleField, accessibleField.getType().asSubclass(KafkaCluster.class), constraints, context));
                            }
                            catch (Throwable t) {
                                ExceptionUtils.throwAsUncheckedException(t);
                            }
                        });

        findFields(testClass,
                field -> predicate.test(field) && Admin.class.isAssignableFrom(field.getType()),
                HierarchyTraversalMode.BOTTOM_UP)
                        .forEach(field -> {
                            try {
                                makeAccessible(field).set(testInstance, getAdmin(
                                        "field " + field.getName(),
                                        field,
                                        field.getType().asSubclass(Admin.class),
                                        context));
                            }
                            catch (Throwable t) {
                                ExceptionUtils.throwAsUncheckedException(t);
                            }
                        });

        findFields(testClass,
                field -> predicate.test(field) && Producer.class.isAssignableFrom(field.getType()),
                HierarchyTraversalMode.BOTTOM_UP)
                        .forEach(field -> {
                            try {
                                makeAccessible(field).set(testInstance, getProducer(
                                        "field " + field.getName(),
                                        field,
                                        (Class) field.getType().asSubclass(Producer.class),
                                        field.getGenericType(),
                                        context));
                            }
                            catch (Throwable t) {
                                ExceptionUtils.throwAsUncheckedException(t);
                            }
                        });
    }

    @Nullable
    private static Serializer<?> getSerializerFromGenericType(Type type, int typeArgumentIndex) {
        Serializer<?> keySerializer = null;
        if (type instanceof ParameterizedType
                && ((ParameterizedType) type).getRawType() instanceof Class<?>
                && Producer.class.isAssignableFrom((Class<?>) ((ParameterizedType) type).getRawType())) {
            // Field declared like Producer<X, Y>, KafkaProducer<X, Y>
            ParameterizedType genericType = (ParameterizedType) type;
            Type key = genericType.getActualTypeArguments()[typeArgumentIndex];
            keySerializer = getSerializerFromType(key);
        }
        return keySerializer;
    }

    private static Serializer<?> getSerializerFromType(Type keyOrValueType) {
        Serializer<?> serializer = null;
        if (keyOrValueType instanceof Class<?>) {
            if (keyOrValueType == String.class) {
                serializer = new StringSerializer();
            }
            else if (keyOrValueType == Integer.class) {
                serializer = new IntegerSerializer();
            }
            else if (keyOrValueType == Long.class) {
                serializer = new LongSerializer();
            }
            else if (keyOrValueType == UUID.class) {
                serializer = new UUIDSerializer();
            }
            else if (keyOrValueType == Float.class) {
                serializer = new FloatSerializer();
            }
            else if (keyOrValueType == Double.class) {
                serializer = new DoubleSerializer();
            }
            else if (keyOrValueType == byte[].class) {
                serializer = new ByteArraySerializer();
            }
            else if (keyOrValueType == ByteBuffer.class) {
                serializer = new ByteBufferSerializer();
            }
            else if (keyOrValueType == Bytes.class) {
                serializer = new BytesSerializer();
            }
            else if (keyOrValueType == Void.class) {
                serializer = new VoidSerializer();
            }
        }
        else if (keyOrValueType instanceof ParameterizedType) {
            if (List.class == ((ParameterizedType) keyOrValueType).getRawType()) {
                return new ListSerializer<>(getSerializerFromType(keyOrValueType));
            }
        }
        return serializer;
    }

    @Nullable
    private static Deserializer<?> getDeserializerFromGenericType(Type type, int typeArgumentIndex) {
        Deserializer<?> deserializer = null;
        if (type instanceof ParameterizedType
                && ((ParameterizedType) type).getRawType() instanceof Class<?>
                && Consumer.class.isAssignableFrom((Class<?>) ((ParameterizedType) type).getRawType())) {
            // Field declared like Producer<X, Y>, KafkaProducer<X, Y>
            ParameterizedType genericType = (ParameterizedType) type;
            Type key = genericType.getActualTypeArguments()[typeArgumentIndex];
            deserializer = getDeserializerFromType(key);
        }
        return deserializer;
    }

    private static Deserializer<?> getDeserializerFromType(Type keyOrValueType) {
        Deserializer<?> deserializer = null;
        if (keyOrValueType instanceof Class<?>) {
            if (keyOrValueType == String.class) {
                deserializer = new StringDeserializer();
            }
            else if (keyOrValueType == Integer.class) {
                deserializer = new IntegerDeserializer();
            }
            else if (keyOrValueType == Long.class) {
                deserializer = new LongDeserializer();
            }
            else if (keyOrValueType == UUID.class) {
                deserializer = new UUIDDeserializer();
            }
            else if (keyOrValueType == Float.class) {
                deserializer = new FloatDeserializer();
            }
            else if (keyOrValueType == Double.class) {
                deserializer = new DoubleDeserializer();
            }
            else if (keyOrValueType == byte[].class) {
                deserializer = new ByteArrayDeserializer();
            }
            else if (keyOrValueType == ByteBuffer.class) {
                deserializer = new ByteBufferDeserializer();
            }
            else if (keyOrValueType == Bytes.class) {
                deserializer = new BytesDeserializer();
            }
            else if (keyOrValueType == Void.class) {
                deserializer = new VoidDeserializer();
            }
        }
        else if (keyOrValueType instanceof ParameterizedType) {
            if (List.class == ((ParameterizedType) keyOrValueType).getRawType()) {
                var ta = ((ParameterizedType) keyOrValueType).getActualTypeArguments()[0];
                if (ta instanceof Class) {
                    return new ListDeserializer<>((Class) ta, getDeserializerFromType(keyOrValueType));
                }
            }
        }
        return deserializer;
    }

    private static Iterable<String> uuidsFrom(String startingPrefix) {
        if (startingPrefix.length() > 22) {
            throw new IllegalArgumentException("startingPrefix is too long to be a Base64-encoded UUID prefix");
        }
        int pad = 22 - startingPrefix.length();
        StringBuilder stringBuilder = new StringBuilder(startingPrefix);
        for (int i = 0; i < pad; i++) {
            stringBuilder.append('0');
        }
        byte[] decode = Base64.getUrlDecoder().decode(stringBuilder.toString());
        var bb = ByteBuffer.wrap(decode);
        var msb = bb.getLong();
        var lsb = bb.getLong();
        return () -> {
            return new Iterator<>() {
                long most = msb;
                long least = lsb;

                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public String next() {
                    var oldLeast = least;
                    if (oldLeast > 0 && least < 0) {
                        // handle overflow: if least overflows we need to increment most
                        most++;
                    }
                    bb.putLong(0, most).putLong(8, least);
                    least++;
                    // avoidRODO allocating Uuid.ZERO and Uuid.
                    return Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());
                }
            };
        };
    }

    private static KafkaCluster findClusterFromContext(
                                                       AnnotatedElement element,
                                                       ExtensionContext extensionContext,
                                                       Class<?> type,
                                                       String description) {

        ExtensionContext.Store store = extensionContext.getStore(CLUSTER_NAMESPACE);
        String clusterName;
        if (element.isAnnotationPresent(Name.class)
                && !element.getAnnotation(Name.class).value().isEmpty()) {
            clusterName = element.getAnnotation(Name.class).value();
        }
        else {
            clusterName = findLastUsedClusterId(store, uuidsFrom(STARTING_PREFIX));
            if (!clusterName.equals(STARTING_PREFIX)) {
                throw new AmbiguousKafkaClusterException(
                        "KafkaCluster to associate with " + description + " is ambiguous, " +
                                "use @Name on the intended cluster and this element to disambiguate");
            }
        }
        LOGGER.log(TRACE, "test {0}: decl {1}: Looking up cluster {2}",
                extensionContext.getUniqueId(),
                element,
                clusterName);
        Closeable<KafkaCluster> last = store.get(clusterName,
                (Class<Closeable<KafkaCluster>>) (Class) Closeable.class);
        Objects.requireNonNull(last);
        return last.get();
    }

    private static KafkaCluster getCluster(AnnotatedElement sourceElement,
                                           Class<? extends KafkaCluster> type,
                                           List<Annotation> constraints,
                                           ExtensionContext extensionContext) {
        // Semantic we want for clients without specified clusterId is "closest enclosing scope"
        // If we used generated keys A, B, C we could get this by iterating lookup from A, B until we found
        // and unused key, and using the last found
        // But if a user-chosen key collided with a generated one then this doesn't work.
        // However users are highly unlikely to chose any given UUID
        // so we just don't start allocating from UUID A, but somewhere random (possibly KCE instance)
        // and reject user-chosen UUIDs in a small range from there
        // This makes the lookup path simple
        // Can also choose where in the UUID space we start (i.e. don't use one of the UUID versions
        // which the user is likely to use when choosing their ID).
        ExtensionContext.Store store = extensionContext.getStore(CLUSTER_NAMESPACE);
        String clusterName;
        if (sourceElement.isAnnotationPresent(Name.class)
                && !sourceElement.getAnnotation(Name.class).value().isEmpty()) {
            clusterName = sourceElement.getAnnotation(Name.class).value();
            Object o = store.get(clusterName);
            if (o != null) {
                throw new ExtensionConfigurationException(
                        "A " + KafkaCluster.class.getSimpleName() + "-typed declaration with @Name(\"" + clusterName + "\") is already in scope");
            }
        }
        else {
            var clusterIdIter = uuidsFrom(STARTING_PREFIX);
            clusterName = findFirstUnusedClusterId(store, clusterIdIter);
        }

        LOGGER.log(TRACE,
                "test {0}: decl {1}: cluster ''{2}'': Looking up cluster",
                extensionContext.getUniqueId(),
                sourceElement,
                clusterName);
        Closeable<KafkaCluster> closeableCluster = store.getOrComputeIfAbsent(clusterName,
                __ -> {
                    return createCluster(extensionContext, clusterName, type, sourceElement, constraints);
                },
                (Class<Closeable<KafkaCluster>>) (Class) Closeable.class);
        Objects.requireNonNull(closeableCluster);
        KafkaCluster cluster = closeableCluster.get();
        LOGGER.log(TRACE,
                "test {0}: decl {1}: cluster ''{2}'': Starting",
                extensionContext.getUniqueId(),
                sourceElement,
                clusterName);
        cluster.start();
        return cluster;
    }

    private static String findFirstUnusedClusterId(ExtensionContext.Store store, Iterable<String> clusterIdIter) {
        var it = clusterIdIter.iterator();
        while (true) {
            String clusterId = it.next();
            var cluster = store.get(clusterId);
            if (cluster == null) {
                return clusterId;
            }
        }
    }

    private static String findLastUsedClusterId(ExtensionContext.Store store, Iterable<String> clusterIdIter) {
        var it = clusterIdIter.iterator();
        String last = null;
        while (true) {
            String clusterId = it.next();
            var cluster = store.get(clusterId);
            if (cluster == null) {
                return last;
            }
            last = clusterId;
        }
    }

    private static Admin getAdmin(String description,
                                  AnnotatedElement sourceElement,
                                  Class<? extends Admin> type,
                                  ExtensionContext extensionContext) {

        KafkaCluster cluster = findClusterFromContext(sourceElement, extensionContext, type, description);

        return extensionContext.getStore(ADMIN_NAMESPACE)
                .<Object, Closeable<Admin>> getOrComputeIfAbsent(sourceElement, __ -> {
                    LOGGER.log(TRACE, "test {0}: decl {1}: Creating Admin",
                            extensionContext.getUniqueId(),
                            sourceElement);
                    return new Closeable<>(sourceElement, cluster.getClusterId(), Admin.create(cluster.getKafkaClientConfiguration()));
                },
                        (Class<Closeable<Admin>>) (Class) Closeable.class)
                .get();
    }

    private static Producer<?, ?> getProducer(String description,
                                              AnnotatedElement sourceElement,
                                              Class<? extends Producer<?, ?>> type,
                                              Type genericType,
                                              ExtensionContext extensionContext) {
        Serializer<?> keySerializer = getSerializerFromGenericType(genericType, 0);
        LOGGER.log(TRACE, "test {0}: decl {1}: key serializer {2}",
                extensionContext.getUniqueId(),
                sourceElement,
                keySerializer);
        Serializer<?> valueSerializer = getSerializerFromGenericType(genericType, 1);
        LOGGER.log(TRACE, "test {0}: decl {1}: value serializer {2}",
                extensionContext.getUniqueId(),
                sourceElement,
                valueSerializer);

        KafkaCluster cluster = findClusterFromContext(sourceElement, extensionContext, type, description);

        return extensionContext.getStore(PRODUCER_NAMESPACE)
                .<Object, Closeable<KafkaProducer<?, ?>>> getOrComputeIfAbsent(sourceElement, __ -> {
                    LOGGER.log(TRACE, "test {0}: decl {1}: Creating KafkaProducer<>",
                            extensionContext.getUniqueId(),
                            sourceElement);
                    return new Closeable<>(sourceElement, cluster.getClusterId(), new KafkaProducer<>(cluster.getKafkaClientConfiguration(),
                            keySerializer, valueSerializer));
                },
                        (Class<Closeable<KafkaProducer<?, ?>>>) (Class) Closeable.class)
                .get();
    }

    private static Consumer<?, ?> getConsumer(String description,
                                              AnnotatedElement sourceElement,
                                              Class<? extends Consumer<?, ?>> type,
                                              Type genericType,
                                              ExtensionContext extensionContext) {
        Deserializer<?> keySerializer = getDeserializerFromGenericType(genericType, 0);
        LOGGER.log(TRACE, "test {0}: decl {1}: key deserializer {2}",
                extensionContext.getUniqueId(),
                sourceElement,
                keySerializer);
        Deserializer<?> valueSerializer = getDeserializerFromGenericType(genericType, 1);
        LOGGER.log(TRACE, "test {0}: decl {1}: value deserializer {2}",
                extensionContext.getUniqueId(),
                sourceElement,
                valueSerializer);

        KafkaCluster cluster = findClusterFromContext(sourceElement, extensionContext, type, description);

        return extensionContext.getStore(CONSUMER_NAMESPACE)
                .<Object, Closeable<KafkaConsumer<?, ?>>> getOrComputeIfAbsent(sourceElement, __ -> {
                    LOGGER.log(TRACE, "test {0}: decl {1}: Creating KafkaConsumer<>",
                            extensionContext.getUniqueId(),
                            sourceElement);
                    return new Closeable<>(sourceElement, cluster.getClusterId(), new KafkaConsumer<>(cluster.getKafkaClientConfiguration(),
                            keySerializer, valueSerializer));
                },
                        (Class<Closeable<KafkaConsumer<?, ?>>>) (Class) Closeable.class)
                .get();
    }

    private static Closeable<KafkaCluster> createCluster(ExtensionContext extensionContext, String clusterName, Class<? extends KafkaCluster> type,
                                                         AnnotatedElement sourceElement,
                                                         List<Annotation> constraints) {
        LOGGER.log(TRACE,
                "test {0}: decl: {1}: cluster ''{2}'': Creating new cluster",
                extensionContext.getUniqueId(),
                sourceElement,
                clusterName);
        LOGGER.log(TRACE,
                "test {0}: decl: {1}: cluster ''{2}'': Constraints {3}",
                extensionContext.getUniqueId(),
                sourceElement,
                clusterName,
                constraints);
        var best = findBestProvisioningStrategy(constraints, type);
        LOGGER.log(TRACE,
                "test {0}: decl: {1}: cluster ''{2}'': Chosen provisioning strategy: {3}",
                extensionContext.getUniqueId(),
                sourceElement,
                clusterName,
                best);
        KafkaCluster c = best.create(constraints, type);
        LOGGER.log(TRACE,
                "test {0}: decl: {1}: cluster ''{2}'': Created",
                extensionContext.getUniqueId(),
                sourceElement,
                clusterName);
        return new Closeable<>(sourceElement, clusterName, c);
    }

    /**
     * @param sourceElement The source element
     * @param metaAnnotationType The meta-annotation
     * @return A mutable list of annotations from the source element that are meta-annotated with
     * the given {@code metaAnnotationType}.
     */
    @NotNull
    private static ArrayList<Annotation> getConstraintAnnotations(AnnotatedElement sourceElement, Class<? extends Annotation> metaAnnotationType) {
        ArrayList<Annotation> constraints;
        if (AnnotationSupport.isAnnotated(sourceElement, metaAnnotationType)) {
            Annotation[] annotations = sourceElement.getAnnotations();
            constraints = filterAnnotations(annotations, metaAnnotationType);
        }
        else {
            constraints = new ArrayList<>();
        }
        return constraints;
    }

    @NotNull
    private static ArrayList<Annotation> filterAnnotations(List<Annotation> annotations,
                                                           Class<? extends Annotation> metaAnnotationType) {
        return filterAnnotations(annotations.stream(), metaAnnotationType);
    }

    @NotNull
    private static ArrayList<Annotation> filterAnnotations(Annotation[] annotations,
                                                           Class<? extends Annotation> metaAnnotationType) {
        return filterAnnotations(Arrays.stream(annotations), metaAnnotationType);
    }

    @NotNull
    private static ArrayList<Annotation> filterAnnotations(Stream<Annotation> annotations,
                                                           Class<? extends Annotation> metaAnnotationType) {
        ArrayList<Annotation> constraints = annotations
                .filter(anno -> anno.annotationType().isAnnotationPresent(metaAnnotationType))
                .collect(Collectors.toCollection(ArrayList::new));
        return constraints;
    }

    static KafkaClusterProvisioningStrategy findBestProvisioningStrategy(
                                                                         List<Annotation> constraints,
                                                                         Class<? extends KafkaCluster> declarationType) {
        ServiceLoader<KafkaClusterProvisioningStrategy> loader = ServiceLoader.load(KafkaClusterProvisioningStrategy.class);
        return loader.stream().map(ServiceLoader.Provider::get)
                .filter(strategy -> {
                    boolean supports = strategy.supportsType(declarationType);
                    if (!supports) {
                        LOGGER.log(TRACE, "Excluding {0} because it is not compatible with declaration of type {1}",
                                strategy, declarationType.getName());
                    }
                    return supports;
                })
                .filter(strategy -> {
                    for (Annotation anno : constraints) {
                        boolean supports = strategy.supportsAnnotation(anno);
                        if (!supports) {
                            LOGGER.log(TRACE, "Excluding {0} because doesn't support {1}",
                                    strategy, anno);
                            return false;
                        }
                    }
                    return true;
                })
                .min(Comparator.comparing(x -> x.estimatedProvisioningTimeMs(constraints, declarationType)))
                .orElseThrow(() -> {
                    var strategies = ServiceLoader.load(KafkaClusterProvisioningStrategy.class).stream().map(ServiceLoader.Provider::type).collect(Collectors.toList());
                    return new ExtensionConfigurationException("No provisioning strategy for a declaration of type " + declarationType.getName()
                            + " and supporting all of " + constraints +
                            " was found (tried: " + classNames(strategies) + ")");
                });
    }

    @NotNull
    private static List<String> classNames(Collection<? extends Class<?>> constraints) {
        return constraints.stream().map(Class::getName).sorted().collect(Collectors.toList());
    }

    private void assertSupportedType(String target, Class<?> type) {
        if (!KafkaCluster.class.isAssignableFrom(type)) {
            throw new ExtensionConfigurationException("Can only resolve declarations of type "
                    + KafkaCluster.class + " but " + target + " has type " + type.getName());
        }
    }

    private class Utils {
        public void assertNodeCount() {

        }
    }
}
