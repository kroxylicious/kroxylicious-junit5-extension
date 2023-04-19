/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.kafka.common;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Functions for creating constraint instances without reflecting on annotated members.
 */
public class ConstraintUtils {
    private ConstraintUtils() {

    }

    /**
     * The Broker cluster constraint instance.
     *
     * @param numBrokers the number of brokers to form the cluster with
     * @return the broker cluster
     */
    public static BrokerCluster brokerCluster(int numBrokers) {
        return mkAnnotation(BrokerCluster.class, Map.of("numBrokers", numBrokers));
    }

    /**
     * The version constraint instance.
     *
     * @param value the value of the version
     * @return the version
     */
    public static Version version(String value) {
        return mkAnnotation(Version.class, Map.of("value", value));
    }

    /**
     *  Creates a constraint to ensure the broker is configured with a particular configuration property.
     *
     * @param name the name
     * @param value the value
     * @return the broker config
     */
    public static BrokerConfig brokerConfig(String name, String value) {
        return mkAnnotation(BrokerConfig.class, Map.of("name", name, "value", value));
    }

    /**
     *  Creates a constraint to ensure the broker is configured with a list of configuration properties.
     *
     * @param configs the configs
     * @return the broker config list
     */
    public static BrokerConfig.List brokerConfigs(Map<String, String> configs) {
        return mkAnnotation(BrokerConfig.List.class, Map.of("value",
                configs.entrySet().stream()
                        .map(entry -> mkAnnotation(BrokerConfig.class, Map.of("name", entry.getKey(), "value", entry.getValue())))
                        .toArray(size -> new BrokerConfig[size])));
    }

    /**
     * The cluster id constraint instance
     *
     * @param clusterId the cluster id
     * @return the cluster id
     */
    public static ClusterId clusterId(String clusterId) {
        return mkAnnotation(ClusterId.class, Map.of("clusterId", clusterId));
    }

    /**
     * Creates a constraint to supply a cluster with a configured number of Kraft controller nodes.
     * <p/>
     * Note this constraint is mutually exclusive with `ZooKeeperCluster` constraint.
     *
     * @param numControllers the number of controllers
     * @return the kraft cluster
     */
    public static KRaftCluster kraftCluster(int numControllers) {
        return mkAnnotation(KRaftCluster.class, Map.of("numControllers", numControllers));
    }

    /**
     * Creates a constraint to supply a cluster using ZooKeeper for controller nodes..     
     * <p/>
     * Note this constraint is mutually exclusive with `ZooKeeperCluster` constraint.
     *
     * @return the zookeeper cluster
     */
    public static ZooKeeperCluster zooKeeperCluster() {
        return mkAnnotation(ZooKeeperCluster.class, Map.of());
    }

    private static <A extends Annotation> A mkAnnotation(Class<A> annoType, Map<String, Object> members) {
        Objects.requireNonNull(members);
        for (String member : members.keySet()) {
            try {
                annoType.getDeclaredMethod(member);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
        Objects.requireNonNull(members);
        return (A) Proxy.newProxyInstance(annoType.getClassLoader(), new Class<?>[]{ annoType },
                new AnnotationProxyInvocationHandler(annoType, members));
    }

    private static class AnnotationProxyInvocationHandler<A extends Annotation> implements InvocationHandler {
        private final Class<A> annoType;
        private final Map<String, Object> members;

        /**
         * Instantiates a new Annotation proxy invocation handler.
         *
         * @param annoType the anno type
         * @param members the members
         */
        public AnnotationProxyInvocationHandler(Class<A> annoType, Map<String, Object> members) {
            this.annoType = annoType;
            this.members = members;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "annotationType" -> {
                    return annoType;
                }
                case "toString" -> {
                    return annoType.getSimpleName() + members.entrySet().stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining(", ", "(", ")"));
                }
                case "hashCode" -> {
                    return members.hashCode();
                }
                case "equals" -> {
                    Object other = args[0];
                    if (Proxy.isProxyClass(other.getClass())) {
                        var otherInvocation = Proxy.getInvocationHandler(other);
                        if (otherInvocation instanceof AnnotationProxyInvocationHandler) {
                            return members.equals(((AnnotationProxyInvocationHandler<?>) otherInvocation).members);
                        }
                    }
                    return false;
                }
                default -> {
                    return members.get(method.getName());
                }
            }
        }
    }
}
