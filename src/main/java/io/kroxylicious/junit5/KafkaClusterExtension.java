/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.junit5;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

import io.kroxylicious.cluster.KafkaCluster;
import io.kroxylicious.cluster.KafkaClusterConfig;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;
import static org.junit.platform.commons.support.ReflectionSupport.findFields;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

/**
 * A Junit5 extension that allows declarative injection of a {@link KafkaCluster} into a test
 * via static or instance field(s) and/or parameters annotated
 * with {@link BrokerCluster @BrokerCluster}.
 *
 * Ssee {@link BrokerCluster @BrokerCluster} for usage examples.
 */
public class KafkaClusterExtension implements
        ParameterResolver, BeforeEachCallback,
        BeforeAllCallback {

    private static final System.Logger LOGGER = System.getLogger(KafkaClusterExtension.class.getName());

    private static final ExtensionContext.Namespace CLUSTER_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, KafkaCluster.class);
    private static final ExtensionContext.Namespace ADMIN_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, Admin.class);
    private static final ExtensionContext.Namespace PRODUCER_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, Producer.class);
    private static final ExtensionContext.Namespace CONSUMER_NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class, Consumer.class);
    public static final String STARTING_PREFIX = "WY9Br5K1vAfov_8jjJ3KUA";

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
        Class<?> type = parameterContext.getParameter().getType();
        if (parameterContext.isAnnotated(BrokerCluster.class) &&
                KafkaCluster.class.isAssignableFrom(type)) {
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
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        if (KafkaCluster.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(KafkaCluster.class);
            return getCluster(parameterContext.getParameter(), paramType, extensionContext);
        }
        else if (Admin.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(Admin.class);
            return getAdmin("parameter " + parameterContext.getParameter().getName(), parameterContext.getParameter(), paramType, extensionContext);
        }
        else if (Producer.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(Producer.class);
            Type paramGenericType = parameterContext.getDeclaringExecutable().getGenericParameterTypes()[parameterContext.getIndex()];
            return getProducer("parameter " + parameterContext.getParameter().getName(), parameterContext.getParameter(), (Class) paramType, paramGenericType,
                    extensionContext);
        }
        else if (Consumer.class.isAssignableFrom(type)) {
            var paramType = type.asSubclass(Consumer.class);
            Type paramGenericType = parameterContext.getDeclaringExecutable().getGenericParameterTypes()[parameterContext.getIndex()];
            return getConsumer("parameter " + parameterContext.getParameter().getName(), parameterContext.getParameter(), (Class) paramType, paramGenericType,
                    extensionContext);
        }
        else {
            throw new ExtensionConfigurationException("Could not resolve " + parameterContext);
        }
    }

    /**
     * Perform field injection for non-private, static fields
     * of type {@link KafkaCluster} or {@link KafkaCluster} that are annotated
     * with {@link BrokerCluster @BrokerCluster}.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        injectStaticFields(context, context.getRequiredTestClass());
    }

    /**
     * Perform field injection for non-private, non-static fields (i.e.,
     * instance fields) of type {@link Path} or {@link File} that are annotated
     * with {@link TempDir @TempDir}.
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
        findAnnotatedFields(
                testClass,
                BrokerCluster.class,
                field -> predicate.test(field) && KafkaCluster.class.isAssignableFrom(field.getType()))
                        .forEach(field -> {
                            assertSupportedType("field", field.getType());
                            try {
                                makeAccessible(field).set(testInstance, getCluster(field, field.getType().asSubclass(KafkaCluster.class), context));
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
        Closeable<KafkaCluster> last = store.get(clusterName,
                (Class<Closeable<KafkaCluster>>) (Class) Closeable.class);
        Objects.requireNonNull(last);
        return last.get();
    }

    private KafkaCluster getCluster(AnnotatedElement sourceElement,
                                    Class<? extends KafkaCluster> type,
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

        Closeable<KafkaCluster> closeableCluster = store.getOrComputeIfAbsent(clusterName,
                __ -> createCluster(clusterName, type, sourceElement),
                (Class<Closeable<KafkaCluster>>) (Class) Closeable.class);
        Objects.requireNonNull(closeableCluster);
        KafkaCluster cluster = closeableCluster.get();
        LOGGER.log(TRACE, "Starting cluster {0} for element {1}",
                clusterName, sourceElement);
        cluster.start();
        return cluster;
    }

    private String findFirstUnusedClusterId(ExtensionContext.Store store, Iterable<String> clusterIdIter) {
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

    private Admin getAdmin(String description,
                           AnnotatedElement sourceElement,
                           Class<? extends Admin> type,
                           ExtensionContext extensionContext) {

        KafkaCluster cluster = findClusterFromContext(sourceElement, extensionContext, type, description);

        return extensionContext.getStore(ADMIN_NAMESPACE)
                .<Object, Closeable<Admin>> getOrComputeIfAbsent(sourceElement, __ -> {
                    return new Closeable<>(sourceElement, cluster.getClusterId(), Admin.create(cluster.getKafkaClientConfiguration()));
                },
                        (Class<Closeable<Admin>>) (Class) Closeable.class)
                .get();
    }

    private Producer<?, ?> getProducer(String description,
                                       AnnotatedElement sourceElement,
                                       Class<? extends Producer<?, ?>> type,
                                       Type genericType,
                                       ExtensionContext extensionContext) {
        Serializer<?> keySerializer = getSerializerFromGenericType(genericType, 0);
        Serializer<?> valueSerializer = getSerializerFromGenericType(genericType, 1);

        KafkaCluster cluster = findClusterFromContext(sourceElement, extensionContext, type, description);

        return extensionContext.getStore(PRODUCER_NAMESPACE)
                .<Object, Closeable<KafkaProducer<?, ?>>> getOrComputeIfAbsent(sourceElement, __ -> {
                    return new Closeable<>(sourceElement, cluster.getClusterId(), new KafkaProducer<>(cluster.getKafkaClientConfiguration(),
                            keySerializer, valueSerializer));
                },
                        (Class<Closeable<KafkaProducer<?, ?>>>) (Class) Closeable.class)
                .get();
    }

    private Consumer<?, ?> getConsumer(String description,
                                       AnnotatedElement sourceElement,
                                       Class<? extends Consumer<?, ?>> type,
                                       Type genericType,
                                       ExtensionContext extensionContext) {
        Deserializer<?> keySerializer = getDeserializerFromGenericType(genericType, 0);
        Deserializer<?> valueSerializer = getDeserializerFromGenericType(genericType, 1);

        KafkaCluster cluster = findClusterFromContext(sourceElement, extensionContext, type, description);

        return extensionContext.getStore(CONSUMER_NAMESPACE)
                .<Object, Closeable<KafkaConsumer<?, ?>>> getOrComputeIfAbsent(sourceElement, __ -> {
                    return new Closeable<>(sourceElement, cluster.getClusterId(), new KafkaConsumer<>(cluster.getKafkaClientConfiguration(),
                            keySerializer, valueSerializer));
                },
                        (Class<Closeable<KafkaConsumer<?, ?>>>) (Class) Closeable.class)
                .get();
    }

    private Closeable<KafkaCluster> createCluster(String clusterName, Class<? extends KafkaCluster> type, AnnotatedElement sourceElement) {
        Set<Class<? extends Annotation>> constraints;
        if (AnnotationSupport.isAnnotated(sourceElement, KafkaClusterConstraint.class)) {
            constraints = Arrays.stream(sourceElement.getAnnotations())
                    .map(Annotation::annotationType)
                    .filter(at -> at.isAnnotationPresent(KafkaClusterConstraint.class)).collect(Collectors.toSet());
        }
        else {
            constraints = Set.of();
        }
        var best = findBestProvisioningStrategy(constraints, type);
        KafkaCluster c = best.create(sourceElement, type);
        return new Closeable<>(sourceElement, clusterName, c);
    }

    static KafkaClusterProvisioningStrategy findBestProvisioningStrategy(
                                                                         Set<? extends Class<? extends Annotation>> constraints,
                                                                         Class<? extends KafkaCluster> declarationType) {
        ServiceLoader<KafkaClusterProvisioningStrategy> loader = ServiceLoader.load(KafkaClusterProvisioningStrategy.class);
        return loader.stream().map(ServiceLoader.Provider::get)
                .filter(strategy -> {
                    boolean supports = strategy.supportsType(declarationType);
                    if (!supports) {
                        LOGGER.log(DEBUG, "Excluding {0} because it is not compatible with declaration of type {1}",
                                strategy, declarationType.getName());
                    }
                    return supports;
                })
                .filter(strategy -> {
                    for (Class<? extends Annotation> anno : constraints) {
                        boolean supports = strategy.supportsAnnotation(anno);
                        if (!supports) {
                            LOGGER.log(DEBUG, "Excluding {0} because doesn't support {1}",
                                    strategy, anno.getName());
                            return false;
                        }
                    }
                    return true;
                })
                .min(Comparator.comparing(KafkaClusterProvisioningStrategy::estimatedProvisioningTimeMs))
                .orElseThrow(() -> {
                    var strategies = ServiceLoader.load(KafkaClusterProvisioningStrategy.class).stream().map(ServiceLoader.Provider::type).collect(Collectors.toList());
                    return new ExtensionConfigurationException("No provisioning strategy for a declaration of type " + declarationType.getName()
                            + " and supporting all of " + classNames(constraints) +
                            " was found (tried: " + classNames(strategies) + ")");
                });
    }

    @NotNull
    private static List<String> classNames(Collection<? extends Class<?>> constraints) {
        return constraints.stream().map(Class::getName).sorted().collect(Collectors.toList());
    }

    public static KafkaClusterConfig kafkaClusterConfig(AnnotatedElement sourceElement) {
        var builder = KafkaClusterConfig.builder()
                .brokersNum(sourceElement.getAnnotation(BrokerCluster.class).numBrokers());
        if (sourceElement.isAnnotationPresent(KRaftCluster.class)
                && sourceElement.isAnnotationPresent(ZooKeeperCluster.class)) {
            throw new ExtensionConfigurationException(
                    "Either @" + KRaftCluster.class.getSimpleName() + " or @" + ZooKeeperCluster.class.getSimpleName() + " can be used, not both");
        }
        else if (sourceElement.isAnnotationPresent(KRaftCluster.class)) {
            var kraft = sourceElement.getAnnotation(KRaftCluster.class);
            builder.kraftMode(true)
                    .kraftControllers(kraft.numControllers());
        }
        else if (sourceElement.isAnnotationPresent(ZooKeeperCluster.class)) {
            builder.kraftMode(false);
            if (sourceElement.isAnnotationPresent(ClusterId.class)
                    && !sourceElement.getAnnotation(ClusterId.class).value().isEmpty()) {
                throw new ExtensionConfigurationException("Specifying @" + ClusterId.class.getSimpleName() + " with @" +
                        ZooKeeperCluster.class.getSimpleName() + " is not supported");
            }
        }
        else {
            builder.kraftMode(true).kraftControllers(1);
        }

        if (sourceElement.isAnnotationPresent(SaslPlainAuth.class)) {
            var authn = sourceElement.getAnnotation(SaslPlainAuth.class);
            builder.saslMechanism("PLAIN");
            builder.users(Arrays.stream(authn.value())
                    .collect(Collectors.toMap(
                            SaslPlainAuth.UserPassword::user,
                            SaslPlainAuth.UserPassword::password)));
        }
        if (sourceElement.isAnnotationPresent(ClusterId.class)
                && !sourceElement.getAnnotation(ClusterId.class).value().isEmpty()) {
            builder.kafkaKraftClusterId(sourceElement.getAnnotation(ClusterId.class).value());
        }
        KafkaClusterConfig build = builder.build();
        return build;
    }

    private void assertSupportedType(String target, Class<?> type) {
        if (!KafkaCluster.class.isAssignableFrom(type)) {
            throw new ExtensionConfigurationException("Can only resolve @" + BrokerCluster.class.getSimpleName() + " declarations of type "
                    + KafkaCluster.class + " but " + target + " has type " + type.getName());
        }
    }
}
