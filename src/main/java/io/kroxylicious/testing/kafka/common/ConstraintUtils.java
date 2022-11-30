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

public class ConstraintUtils {
    private ConstraintUtils() {

    }

    public static BrokerCluster brokerCluster(int numBrokers) {
        return mkAnnotation(BrokerCluster.class, Map.of("numBrokers", numBrokers));
    }

    public static BrokerConfig brokerConfig(String name, String value) {
        return mkAnnotation(BrokerConfig.class, Map.of("name", name, "value", value));
    }

    public static BrokerConfig.List brokerConfigs(Map<String, String> configs) {
        return mkAnnotation(BrokerConfig.List.class, Map.of("value",
                configs.entrySet().stream()
                        .map(entry -> mkAnnotation(BrokerConfig.class, Map.of("name", entry.getKey(), "value", entry.getValue())))
                        .toArray(size -> new BrokerConfig[size])));
    }

    public static ClusterId clusterId(String clusterId) {
        return mkAnnotation(ClusterId.class, Map.of("clusterId", clusterId));
    }

    public static KRaftCluster kraftCluster(int numControllers) {
        return mkAnnotation(KRaftCluster.class, Map.of("numControllers", numControllers));
    }

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

        public AnnotationProxyInvocationHandler(Class<A> annoType, Map<String, Object> members) {
            this.annoType = annoType;
            this.members = members;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("annotationType".equals(method.getName())) {
                return annoType;
            }
            else if ("toString".equals(method.getName())) {
                return annoType.getSimpleName() + members.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(", ", "(", ")"));
            }
            else if ("hashCode".equals(method.getName())) {
                return members.hashCode();
            }
            else if ("equals".equals(method.getName())) {
                Object other = args[0];
                if (Proxy.isProxyClass(other.getClass())) {
                    var otherInvocation = Proxy.getInvocationHandler(other);
                    if (otherInvocation instanceof AnnotationProxyInvocationHandler) {
                        return members.equals(((AnnotationProxyInvocationHandler<?>) otherInvocation).members);
                    }
                }
                return false;
            }
            else {
                return members.get(method.getName());
            }
        }
    }
}
