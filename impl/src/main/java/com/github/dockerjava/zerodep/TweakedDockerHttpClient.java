/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.dockerjava.zerodep;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.transport.NamedPipeSocket;
import com.github.dockerjava.transport.SSLConfig;
import com.github.dockerjava.transport.UnixSocket;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.config.RequestConfig;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.ConnectionClosedException;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.ContentLengthStrategy;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.Header;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpHeaders;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpHost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.NameValuePair;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.config.Registry;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.config.RegistryBuilder;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.impl.DefaultContentLengthStrategy;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.impl.io.EmptyInputStream;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.SocketConfig;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.InputStreamEntity;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.protocol.BasicHttpContext;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.protocol.HttpContext;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.net.URIAuthority;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.util.TimeValue;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.util.Timeout;

public class TweakedDockerHttpClient implements DockerHttpClient {

    private final CloseableHttpClient httpClient;
    private final HttpHost host;
    private final String pathPrefix;

    public TweakedDockerHttpClient(
                                   URI dockerHost,
                                   SSLConfig sslConfig,
                                   int maxConnections,
                                   int maxConnectionsPerRoute,
                                   Duration connectionTimeout,
                                   Duration responseTimeout,
                                   Duration connectionKeepAlive,
                                   int maxRetries,
                                   Duration retryInterval) {
        Registry<ConnectionSocketFactory> socketFactoryRegistry = createConnectionSocketFactoryRegistry(sslConfig, dockerHost);

        switch (dockerHost.getScheme()) {
            case "unix":
            case "npipe":
                pathPrefix = "";
                host = new HttpHost(dockerHost.getScheme(), "localhost", 2375);
                break;
            case "tcp":
                String rawPath = dockerHost.getRawPath();
                pathPrefix = rawPath.endsWith("/")
                        ? rawPath.substring(0, rawPath.length() - 1)
                        : rawPath;
                host = new HttpHost(
                        socketFactoryRegistry.lookup("https") != null ? "https" : "http",
                        dockerHost.getHost(),
                        dockerHost.getPort());
                break;
            default:
                throw new IllegalArgumentException("Unsupported protocol scheme: " + dockerHost);
        }

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                socketFactoryRegistry,
                new ManagedHttpClientConnectionFactory(
                        null,
                        null,
                        null,
                        null,
                        message -> {
                            Header transferEncodingHeader = message.getFirstHeader(HttpHeaders.TRANSFER_ENCODING);
                            if (transferEncodingHeader != null) {
                                if ("identity".equalsIgnoreCase(transferEncodingHeader.getValue())) {
                                    return ContentLengthStrategy.UNDEFINED;
                                }
                            }
                            return DefaultContentLengthStrategy.INSTANCE.determineLength(message);
                        },
                        null));
        // See https://github.com/docker-java/docker-java/pull/1590#issuecomment-870581289
        connectionManager.setDefaultSocketConfig(
                SocketConfig.copy(SocketConfig.DEFAULT)
                        .setSoTimeout(Timeout.ZERO_MILLISECONDS)
                        .build());
        connectionManager.setValidateAfterInactivity(TimeValue.NEG_ONE_SECOND);
        connectionManager.setMaxTotal(maxConnections);
        connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);
        RequestConfig.Builder defaultRequest = RequestConfig.custom();
        if (connectionTimeout != null) {
            defaultRequest.setConnectTimeout(connectionTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        if (responseTimeout != null) {
            defaultRequest.setResponseTimeout(responseTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        if (connectionKeepAlive != null) {
            defaultRequest.setDefaultKeepAlive(connectionKeepAlive.toNanos(), TimeUnit.NANOSECONDS);
        }

        TimeValue retryInt = retryInterval != null ? TimeValue.ofNanoseconds(retryInterval.toNanos()) : TimeValue.ofSeconds(1L);
        DefaultHttpRequestRetryStrategy retryStrategy = new DefaultHttpRequestRetryStrategy(maxRetries, retryInt);
        httpClient = HttpClients.custom()
                .setRequestExecutor(new HijackingHttpRequestExecutor(null))
                .setConnectionManager(connectionManager)
                .setRetryStrategy(retryStrategy)
                .setDefaultRequestConfig(defaultRequest.build())
                .disableConnectionState()
                .build();
    }

    private Registry<ConnectionSocketFactory> createConnectionSocketFactoryRegistry(
                                                                                    SSLConfig sslConfig,
                                                                                    URI dockerHost) {
        RegistryBuilder<ConnectionSocketFactory> socketFactoryRegistryBuilder = RegistryBuilder.create();

        if (sslConfig != null) {
            try {
                SSLContext sslContext = sslConfig.getSSLContext();
                if (sslContext != null) {
                    socketFactoryRegistryBuilder.register("https", new SSLConnectionSocketFactory(sslContext));
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return socketFactoryRegistryBuilder
                .register("tcp", PlainConnectionSocketFactory.INSTANCE)
                .register("http", PlainConnectionSocketFactory.INSTANCE)
                .register("unix", new PlainConnectionSocketFactory() {
                    @Override
                    public Socket createSocket(HttpContext context) throws IOException {
                        return UnixSocket.get(dockerHost.getPath());
                    }
                })
                .register("npipe", new PlainConnectionSocketFactory() {
                    @Override
                    public Socket createSocket(HttpContext context) {
                        return new NamedPipeSocket(dockerHost.getPath());
                    }
                })
                .build();
    }

    @Override
    public Response execute(Request request) {
        HttpContext context = new BasicHttpContext();
        HttpUriRequestBase httpUriRequest = new HttpUriRequestBase(request.method(), URI.create(pathPrefix + request.path()));
        httpUriRequest.setScheme(host.getSchemeName());
        httpUriRequest.setAuthority(new URIAuthority(host.getHostName(), host.getPort()));

        request.headers().forEach(httpUriRequest::addHeader);

        byte[] bodyBytes = request.bodyBytes();
        if (bodyBytes != null) {
            httpUriRequest.setEntity(new ByteArrayEntity(bodyBytes, null));
        }
        else {
            InputStream body = request.body();
            if (body != null) {
                httpUriRequest.setEntity(new InputStreamEntity(body, null));
            }
        }

        if (request.hijackedInput() != null) {
            context.setAttribute(HijackingHttpRequestExecutor.HIJACKED_INPUT_ATTRIBUTE, request.hijackedInput());
            httpUriRequest.setHeader("Upgrade", "tcp");
            httpUriRequest.setHeader("Connection", "Upgrade");
        }

        try {
            CloseableHttpResponse response = httpClient.execute(host, httpUriRequest, context);

            return new ApacheResponse(httpUriRequest, response);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    static class ApacheResponse implements Response {

        private static final Logger LOGGER = LoggerFactory.getLogger(ApacheResponse.class);

        private final HttpUriRequestBase request;

        private final CloseableHttpResponse response;

        ApacheResponse(HttpUriRequestBase httpUriRequest, CloseableHttpResponse response) {
            this.request = httpUriRequest;
            this.response = response;
        }

        @Override
        public int getStatusCode() {
            return response.getCode();
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return Stream.of(response.getHeaders()).collect(Collectors.groupingBy(
                    NameValuePair::getName,
                    Collectors.mapping(NameValuePair::getValue, Collectors.toList())));
        }

        @Override
        public String getHeader(String name) {
            Header firstHeader = response.getFirstHeader(name);
            return firstHeader != null ? firstHeader.getValue() : null;
        }

        @Override
        public InputStream getBody() {
            try {
                return response.getEntity() != null
                        ? response.getEntity().getContent()
                        : EmptyInputStream.INSTANCE;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            try {
                request.abort();
            }
            catch (Exception e) {
                LOGGER.debug("Failed to abort the request", e);
            }

            try {
                response.close();
            }
            catch (ConnectionClosedException e) {
                LOGGER.trace("Failed to close the response", e);
            }
            catch (Exception e) {
                LOGGER.debug("Failed to close the response", e);
            }
        }
    }
}
