package com.compcruz;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.netty.channel.AbortedException;
import reactor.netty.http.client.PrematureCloseException;
import reactor.netty.resources.ConnectionProvider;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Objects;
import java.util.Random;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorResourceFactory;
import org.springframework.util.SocketUtils;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;

import io.netty.handler.timeout.ReadTimeoutException;

import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@Slf4j
public class WebClientComposerTest  {

    private static final int MAX_RETRIES = 5;
    private static final int REACTOR_NETTY_RETRY_FACTOR_FOR_CONNECTION_RESET = 2; // double, since netty also retries once on our retry
    private static final int MAX_SETUP_TRIES = 3;
    private static final int MIN_PORT = 40000;
    private static final int MAX_PORT = 49999;
    private static final String HOST = "localhost";

    private static final String PATH = "/status";
    private static final String RESPONSE_BODY = "{ \"response\": \"body\"}";
    private static final String ERROR_MESSAGE = "Bad Request";
    private static final String CUST_HEADER_NAME = "customeHeader";
    private static final String CUST_HEADER_VALUE = "someValue";

    private WireMockServer targetServer;
    private WireMock targetServerWireMock;
    private String targetServerBaseUrl;

    private ReactorResourceFactory reactorResourceFactory;
    private WebClientComposer webClientComposer;
    private WebClientComposer webClientComposerWithReadTimeout;

    @BeforeClass(alwaysRun = true)
    public void setUp() {
        this.targetServer = createWireMockServer(WireMockConfiguration.wireMockConfig().enableBrowserProxying(false));
        final int targetServerPort = this.targetServer.port();
        this.targetServer.start();
        this.targetServerWireMock = new WireMock(targetServerPort);
        this.targetServerBaseUrl = String.format("http://%s:%d", HOST, targetServerPort);
    }

    @BeforeMethod(alwaysRun = true)
    public void init() {
        this.reactorResourceFactory = getDefaultReactorResourceFactory();

        this.webClientComposer = WebClientComposer.builder()
                .baseUrl(this.targetServerBaseUrl)
                .reactorResourceFactory(this.reactorResourceFactory)
                .build();

        this.webClientComposerWithReadTimeout = WebClientComposer.builder()
                .readTimeoutSecs(1)
                .baseUrl(this.targetServerBaseUrl)
                .reactorResourceFactory(this.reactorResourceFactory)
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void reset() {
        this.reactorResourceFactory.destroy();
        this.targetServerWireMock.resetMappings();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        this.targetServerWireMock.shutdown();
        this.targetServer.stop();
    }

    @Test
    public void whenInvokeWebClientSuccessWithoutProxy() {
        this.targetServerWireMock.register(WireMock.get(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withBody(RESPONSE_BODY)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.OK.value())));
        this.whenInvokeWebClientSuccess(this.webClientComposer.getWebClient());
    }

    @Test
    public void whenInvokeWebClientCustomFilter() {
        final ExchangeFilterFunction custHeaderExchangeFilter = ExchangeFilterFunction.ofRequestProcessor(clientRequest ->
                Mono.just(ClientRequest.from(clientRequest)
                    .headers(httpHeaders -> httpHeaders.put(CUST_HEADER_NAME, Collections.singletonList(CUST_HEADER_VALUE)))
                    .build()));
        final WebClientComposer clientComposer = WebClientComposer.builder()
                .requestFilter(custHeaderExchangeFilter)
                .baseUrl(this.targetServerBaseUrl)
                .reactorResourceFactory(getDefaultReactorResourceFactory())
                .build();

        final RequestPatternBuilder reqPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        final MappingBuilder mappingBuilder = WireMock.get(WireMock.urlEqualTo(reqPatternBuilder.build().getUrl()));
        mappingBuilder.withHeader(CUST_HEADER_NAME, WireMock.containing(CUST_HEADER_VALUE))
                .willReturn(WireMock.aResponse()
                        .withBody(RESPONSE_BODY)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.OK.value()));
        this.targetServerWireMock.register(mappingBuilder);

        final ClientResponse actualResponse = clientComposer
                .getWebClient()
                .get()
                .uri(PATH)
                .exchange()
                .block();
        final String responseBody = actualResponse.bodyToMono(String.class)
                .block();
        assertThat(actualResponse.statusCode(), is(HttpStatus.OK));
        assertThat(responseBody, is(RESPONSE_BODY));
    }

    @Test
    public void whenInvokeWebClientBadRequest() {
        this.targetServerWireMock.register(WireMock.get(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withBody(ERROR_MESSAGE)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.BAD_REQUEST.value())));
        final ClientResponse actualResponse = this.webClientComposer.getWebClient().get().uri(PATH).exchange().block();
        final String responseBody = actualResponse.bodyToMono(String.class).block();

        assertThat(actualResponse.statusCode(), is(HttpStatus.BAD_REQUEST));
        assertThat(responseBody, is(ERROR_MESSAGE));
    }

    @DataProvider
    private Object[][] retriedHttpStatus() {
        return new Object[][] {
                { HttpStatus.BAD_GATEWAY, },
                { HttpStatus.GATEWAY_TIMEOUT, },
                { HttpStatus.SERVICE_UNAVAILABLE, },
        };
    }

    @Test(dataProvider = "retriedHttpStatus")
    public void whenMultipleRetriesSucceedsForRetriedHttpStatus(final HttpStatus httpStatus) {
        final MappingBuilder firstStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .willReturn(WireMock.aResponse()
                        .withBody(ERROR_MESSAGE)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(httpStatus.value()))
                .willSetStateTo("firstFailure");
        this.targetServerWireMock.register(firstStubbing);

        final MappingBuilder secondStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .whenScenarioStateIs("firstFailure")
                .willReturn(WireMock.aResponse()
                        .withBody(ERROR_MESSAGE)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(httpStatus.value()))
                .willSetStateTo("secondFailure");
        this.targetServerWireMock.register(secondStubbing);

        final MappingBuilder thirdStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .whenScenarioStateIs("secondFailure")
                .willReturn(WireMock.aResponse()
                        .withBody(RESPONSE_BODY)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.OK.value()));
        this.targetServerWireMock.register(thirdStubbing);

        final RequestRetryConditions requestRetryConditions = RequestRetryConditions.builder()
                .maxRetries(3)
                .build();
        final String responseBody = this.webClientComposer
                .getWebClient()
                .get()
                .uri(PATH)
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(requestRetryConditions.retryWithFixedBackoff())
                .block();

        final RequestPatternBuilder expectedPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        this.targetServerWireMock.verifyThat(3, expectedPatternBuilder);
        assertThat(responseBody, is(RESPONSE_BODY));
    }

    @DataProvider
    private Object[][] errorHttpStatus() {
        return new Object[][] {
                { HttpStatus.BAD_GATEWAY, },
                { HttpStatus.GATEWAY_TIMEOUT, },
                { HttpStatus.SERVICE_UNAVAILABLE, },
        };
    }

    @Test(dataProvider = "errorHttpStatus")
    public void whenMultipleRetriesFailsForVariousHttpStatuses(final HttpStatus httpStatus) {
        final MappingBuilder firstStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withBody(ERROR_MESSAGE)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(httpStatus.value()));
        this.targetServerWireMock.register(firstStubbing);

        final RequestRetryConditions requestRetryConditions = RequestRetryConditions.builder()
                .maxRetries(3)
                .build();

        boolean exceptionOccurred = false;
        try {
            this.webClientComposer
                    .getWebClient()
                    .get()
                    .uri(PATH)
                    .retrieve()
                    .bodyToMono(String.class)
                    .retryWhen(requestRetryConditions.retryWithFixedBackoff())
                    .block();
        } catch (final Exception ex) {
            exceptionOccurred = true;
            assertThat(Exceptions.isRetryExhausted(ex), is(true));
            assertThat(WebClientResponseException.class.isInstance(ex.getCause()), is(true));
        }

        assertThat(exceptionOccurred, is(true));
        final RequestPatternBuilder expectedPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        this.targetServerWireMock.verifyThat(4, expectedPatternBuilder);
    }

    @Test
    public void whenMultipleRetriesFailsForBadGateway() {
        final MappingBuilder firstStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .willReturn(WireMock.aResponse()
                        .withBody(ERROR_MESSAGE)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.BAD_GATEWAY.value()));
        this.targetServerWireMock.register(firstStubbing);

        final RequestRetryConditions requestRetryConditions = RequestRetryConditions.builder()
                .maxRetries(MAX_RETRIES)
                .build();

        boolean exceptionOccurred = false;
        try {
            this.webClientComposer
                    .getWebClient()
                    .get()
                    .uri(PATH)
                    .retrieve()
                    .bodyToMono(String.class)
                    .retryWhen(requestRetryConditions.retryWithFixedBackoff())
                    .block();
        } catch (final Exception ex) {
            exceptionOccurred = true;
            assertThat(Exceptions.isRetryExhausted(ex), is(true));
            assertThat(WebClientResponseException.class.isInstance(ex.getCause()), is(true));
        }
        assertThat(exceptionOccurred, is(true));
        final RequestPatternBuilder expectedPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        this.targetServerWireMock.verifyThat(MAX_RETRIES + 1, expectedPatternBuilder);
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "^reactorResourceFactory is marked non-null but is null$")
    public void defaultBuilder() {
        assertThat(WebClientComposer.builder().build(), notNullValue());
    }

    @Test
    public void whenMultipleRetriesSucceedsForPrematureCloseException() {
        final MappingBuilder firstStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .willReturn(WireMock.aResponse()
                        .withFault(Fault.EMPTY_RESPONSE))
                .willSetStateTo("firstFailure");
        this.targetServerWireMock.register(firstStubbing);

        final MappingBuilder secondStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .whenScenarioStateIs("firstFailure")
                .willReturn(WireMock.aResponse()
                        .withFault(Fault.EMPTY_RESPONSE))
                .willSetStateTo("secondFailure");
        this.targetServerWireMock.register(secondStubbing);

        final MappingBuilder thirdStubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .whenScenarioStateIs("secondFailure")
                .willReturn(WireMock.aResponse()
                        .withBody(RESPONSE_BODY)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.OK.value()));
        this.targetServerWireMock.register(thirdStubbing);

        final RequestRetryConditions requestRetryConditions = RequestRetryConditions.builder()
                .maxRetries(3)
                .build();

        final String responseBody = this.webClientComposer
                .getWebClient()
                .get()
                .uri(PATH)
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(requestRetryConditions.retryWithFixedBackoff())
                .block();

        final RequestPatternBuilder expectedPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        this.targetServerWireMock.verifyThat(3, expectedPatternBuilder);
        assertThat(responseBody, is(RESPONSE_BODY));
    }

    @DataProvider
    Object[][] nonHttpRetriableExceptions() {
        final Predicate<Throwable> connectionResetPredicate = AbortedException::isConnectionReset;
        final Predicate<Throwable> prematureClosePredicate = throwable -> PrematureCloseException.class.isAssignableFrom(throwable.getClass());
        return new Object[][] {
                // Exception, assertion predicate, retry count for validation
                { Fault.EMPTY_RESPONSE, prematureClosePredicate, MAX_RETRIES + 1 },
                { Fault.CONNECTION_RESET_BY_PEER, connectionResetPredicate, (MAX_RETRIES + 1) * REACTOR_NETTY_RETRY_FACTOR_FOR_CONNECTION_RESET},
        };
    }

    @Test(dataProvider = "nonHttpRetriableExceptions")
    public void whenMultipleRetriesFailsRetriableExceptions(final Fault fault, final Predicate<Throwable> predicate, final int retryCount) {
        final MappingBuilder stubbing = WireMock.get(WireMock.urlEqualTo(PATH))
                .inScenario("test100")
                .willReturn(WireMock.aResponse().withFault(fault));
        this.targetServerWireMock.register(stubbing);

        final RequestRetryConditions requestRetryConditions = RequestRetryConditions.builder()
                .maxRetries(MAX_RETRIES)
                .build();

        boolean exceptionOccurred = false;
        Exception exception = null;
        try {
            this.webClientComposer
                    .getWebClient()
                    .get()
                    .uri(PATH)
                    .retrieve()
                    .bodyToMono(String.class)
                    .retryWhen(requestRetryConditions.retryWithFixedBackoff())
                    .block();
        } catch (final RuntimeException ex) {
            exceptionOccurred = true;
            exception = ex;
        }
        assertThat(exceptionOccurred, is(true));
        exception = Objects.requireNonNull(exception);
        assertThat(exception.getClass().getSimpleName(), is("RetryExhaustedException"));
        assertThat(predicate.test(exception.getCause()), is(true));
        final RequestPatternBuilder expectedPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        this.targetServerWireMock.verifyThat(retryCount, expectedPatternBuilder);
    }

    @Test
    public void builderWithResourceFactory() {
        final WebClientComposer c = WebClientComposer.builder()
                .reactorResourceFactory(this.reactorResourceFactory)
                .build();
        assertThat(c.getBaseUrl(), is(StringUtils.EMPTY));
        assertThat(c.getConnectionTimeout(), is(WebClientComposer.DEFAULT_CONNECTION_TIMEOUT_MILLIS));
        assertThat(c.getReadTimeoutSecs(), is(0));
        assertThat(c.getMaxPoolConnectionSize(), is(ConnectionProvider.DEFAULT_POOL_MAX_CONNECTIONS));
        assertThat(c.getReactorResourceFactory(), is(this.reactorResourceFactory));
        assertThat(c.getRequestFilters(), hasSize(0));
        assertThat(c.getResponseFilters(), hasSize(0));
        assertThat(c.isSocketKeepAlive(), is(WebClientComposer.DEFAULT_SOCKET_KEEP_ALIVE_ENABLED));
    }

    @Test(expectedExceptions = ReadTimeoutException.class)
    public void whenInvokeWebClientReadTimeoutException() {
        this.targetServerWireMock.register(WireMock.get(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withFixedDelay(2000)
                        .withBody(RESPONSE_BODY)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.OK.value())));

        this.webClientComposerWithReadTimeout.getWebClient().get().uri(PATH).exchange().block();
    }

    @DataProvider
    private Object[][] webclientDataProvider() {
        final WebClient webClient = WebClientComposer.builder()
                .baseUrl(this.targetServerBaseUrl)
                .maxInMemorySize(500)
                .reactorResourceFactory(getDefaultReactorResourceFactory())
                .build()
                .getWebClient();
        return new Object[][] {
                { webClient, },
                { webClient.mutate().build(), },
                { webClient.mutate().build().mutate().build(), },
        };
    }

    @Test(dataProvider = "webclientDataProvider", expectedExceptions = DataBufferLimitException.class,
            expectedExceptionsMessageRegExp = "(?s).*Exceeded limit on max bytes to buffer(?s).*")
    public void testDataBufferLimitException(final WebClient webClient) {
        final byte[] array = new byte[400];
        new Random().nextBytes(array);
        this.targetServerWireMock.register(WireMock.get(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withBody(new String(array, Charset.forName("UTF-8")))
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.INTERNAL_SERVER_ERROR.value())));

        final ClientResponse actualResponse = webClient
                .get()
                .uri(PATH)
                .exchange()
                .block();

        actualResponse.bodyToMono(String.class).block();
    }

    private WireMockServer createWireMockServer(final WireMockConfiguration wireMockConfiguration) {
        for (int index = 0; index < MAX_SETUP_TRIES; index++) {
            final int port = SocketUtils.findAvailableTcpPort(MIN_PORT, MAX_PORT);

            try {
                final WireMockServer mockServer = new WireMockServer(wireMockConfiguration.port(port));

                log.info("Starting server {}:{}", HOST, port);
                mockServer.start();
                return mockServer;
            } catch (final RuntimeException exn) {
                log.error("Error starting server {}:{} on try {}", HOST, port, index);
            }
        }
        throw new IllegalStateException("Unable to start server after " + MAX_SETUP_TRIES + " attempts.");
    }

    private void whenInvokeWebClientSuccess(final WebClient webClient) {
        this.targetServerWireMock.register(WireMock.get(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withBody(RESPONSE_BODY)
                        .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .withStatus(HttpStatus.OK.value())));

        final ClientResponse actualResponse = webClient
                .get()
                .uri(PATH)
                .exchange()
                .block();
        final String responseBody = actualResponse.bodyToMono(String.class)
                .block();
        assertThat(actualResponse.statusCode(), is(HttpStatus.OK));
        assertThat(responseBody, is(RESPONSE_BODY));
        final RequestPatternBuilder expectedPatternBuilder = RequestPatternBuilder.newRequestPattern(RequestMethod.GET, WireMock.urlEqualTo(PATH));
        this.targetServerWireMock.verifyThat(1, expectedPatternBuilder);
    }

    private static ReactorResourceFactory getDefaultReactorResourceFactory() {
        final ReactorResourceFactory reactorResourceFactory = new ReactorResourceFactory();
        reactorResourceFactory.setUseGlobalResources(false);
        reactorResourceFactory.setConnectionProvider(ConnectionProvider.builder("some-name").build());
        reactorResourceFactory.afterPropertiesSet();
        return reactorResourceFactory;
    }
}
