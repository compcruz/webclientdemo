package com.compcruz;

import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import java.util.List;
import java.util.function.Consumer;

import javax.net.ssl.SSLException;

import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.client.reactive.ReactorResourceFactory;
import org.springframework.http.codec.ClientCodecConfigurer;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.ReadTimeoutHandler;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * Instantiates a reactive {@link WebClient} to invoke HTTP call in reactive fashion. During JVM shutdown, the consumers
 * of this common library should be mindful of closing the resources. {@link ReactorResourceFactory} should be declared
 * as a bean so that spring will properly cleanup resources on shutdown.
 */
@Slf4j
@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WebClientComposer {

    static final boolean DEFAULT_SOCKET_KEEP_ALIVE_ENABLED = true;
    static final boolean DEFAULT_HTTP_PROXY_ENABLED = false;
    static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 5000;
    static final int MAX_IN_MEMORY_SIZE_BYTES = 1024 * 1024;

    @Builder.Default
    private final String baseUrl = "";

    @Builder.Default
    private final int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MILLIS;

    @Builder.Default
    private final int readTimeoutSecs = 0;

    @Builder.Default
    private final boolean socketKeepAlive = DEFAULT_SOCKET_KEEP_ALIVE_ENABLED;

    @Builder.Default
    private final int maxInMemorySize = MAX_IN_MEMORY_SIZE_BYTES;

    @Builder.Default
    private final int maxPoolConnectionSize = ConnectionProvider.DEFAULT_POOL_MAX_CONNECTIONS;

    @Singular
    private final List<ExchangeFilterFunction> requestFilters;

    @Singular
    private final List<ExchangeFilterFunction> responseFilters;

    @NonNull
    private final ReactorResourceFactory reactorResourceFactory;

    @NonNull
    private final SslContext sslContext = getSslContext();

    /**
     * Returns an instance of reactive web client with all needed properties configured.
     *
     * @return An instance of {@link WebClient}
     */
    public WebClient getWebClient() {
        final Consumer<ClientCodecConfigurer> codec = configurer ->
                configurer.defaultCodecs().maxInMemorySize(this.maxInMemorySize);
        return WebClient.builder()
                .baseUrl(this.baseUrl)
                .codecs(codec)
                .filters(exchangeFilterFunctions -> exchangeFilterFunctions.addAll(this.requestFilters))
                .filters(exchangeFilterFunctions -> exchangeFilterFunctions.addAll(this.responseFilters))
                .clientConnector(getClientHttpConnector())
                .build();
    }

    private ClientHttpConnector getClientHttpConnector() {
        return new ReactorClientHttpConnector(this.reactorResourceFactory, httpClient -> httpClient.tcpConfiguration(tcpClient -> {
            TcpClient enhancedTcpClient = tcpClient;
            enhancedTcpClient = enhancedTcpClient.secure(t -> t.sslContext(this.sslContext))
                    .option(ChannelOption.SO_KEEPALIVE, this.socketKeepAlive)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, this.connectionTimeout);
            if (this.readTimeoutSecs > 0) {
                enhancedTcpClient = enhancedTcpClient.doOnConnected(connection -> connection.addHandlerLast(new ReadTimeoutHandler(this.readTimeoutSecs)));
            }
            return enhancedTcpClient;
        }));
    }

    private SslContext getSslContext() {
        try {
            return SslContextBuilder.forClient().build();
        } catch (final SSLException ex) {
            throw new IllegalStateException("Unable to create SSL context", ex);
        }
    }
}
