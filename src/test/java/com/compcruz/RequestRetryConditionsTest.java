package com.compcruz;

import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.net.ssl.SSLException;

import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.channel.AbortedException;
import reactor.netty.http.client.PrematureCloseException;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class RequestRetryConditionsTest {

    private static final SSLException SSL_EXCEPTION = new SSLException("Something went wrong");
    private static final String THROWABLE_MSG = "General Error:[We encountered a backend error. Please try again.]";

    private Queue<reactor.util.retry.Retry.RetrySignal> retrySignals;

    @BeforeMethod(alwaysRun = true)
    public void init() {
        this.retrySignals = new ConcurrentLinkedQueue<>();
    }

    @AfterMethod(alwaysRun = true)
    public void reset() {
        this.retrySignals.clear();
    }

    @Test
    public void monoRetryNoBackoff() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().maxRetries(2).build()
                .retryWithNoBackoff()
                .doBeforeRetry(this.doOnRetryNew());

        StepVerifier.create(Mono.error(SSL_EXCEPTION)
                .retryWhen(retry))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(2, SSLException.class));
    }

    @Test
    public void fluxRetryNoBackoff() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().maxRetries(2).build()
                .retryWithNoBackoff()
                .doBeforeRetry(this.doOnRetryNew());

        final Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(SSL_EXCEPTION)).retryWhen(retry);

        StepVerifier.create(flux)
                .expectNext(0, 1, 0, 1, 0, 1)
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(2, SSLException.class));
    }

    @Test
    public void fluxRetryFixedBackoff() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().maxRetries(2).fixedDelayMillis(500).build()
                .retryWithFixedBackoff().doBeforeRetry(this.doOnRetryNew());

        final Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(SSL_EXCEPTION)).retryWhen(retry);

        StepVerifier.withVirtualTime(() -> flux)
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0, 1)
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(2, SSLException.class));
    }

    @Test
    public void monoRetryFixedBackoff() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().maxRetries(1).fixedDelayMillis(500).build()
                .retryWithFixedBackoff().doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() -> Mono.error(SSL_EXCEPTION).retryWhen(retry))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(500))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(1, SSLException.class));
    }

    @Test
    public void monoRetryExponentialBackoff() {
        final RetryBackoffSpec retryNew = RequestRetryConditions.builder().maxRetries(5).build()
                .retryWithExponentialBackoff()
                .doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() -> Mono.error(SSL_EXCEPTION).retryWhen(retryNew))
                .expectSubscription()
                // As we know for each ith retry -nextBackoff = minBackoffDelayMillis*pow(2,i) so pause for sum of nextBackoff of all the retries.
                .thenAwait(Duration.ofMillis(100))
                .thenAwait(Duration.ofMillis(200))
                .thenAwait(Duration.ofMillis(400))
                .thenAwait(Duration.ofMillis(800))
                .thenAwait(Duration.ofMillis(1600))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(5, SSLException.class));
    }

    @Test
    public void monoRetryExponentialBackoffWhenNextBackoffReachesMaxBackoffDelay() {
        final RetryBackoffSpec retryNew = RequestRetryConditions.builder().maxBackoffDelayMillis(400).maxRetries(5).build()
                .retryWithExponentialBackoff()
                .doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() -> Mono.error(SSL_EXCEPTION).retryWhen(retryNew))
                .expectSubscription()
                // As we know for each ith retry,nextBackoff = minBackoffDelayMillis*pow(2,i) so pause for sum of nextBackoff of all the retries.
                .thenAwait(Duration.ofMillis(100))
                .thenAwait(Duration.ofMillis(200))
                .thenAwait(Duration.ofMillis(400))
                .thenAwait(Duration.ofMillis(400))
                .thenAwait(Duration.ofMillis(400))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(5, SSLException.class));
    }

    @Test
    public void fluxRetryExponentialBackoff() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().maxRetries(4).build()
                .retryWithExponentialBackoff()
                .doBeforeRetry(this.doOnRetryNew());

        final Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(SSL_EXCEPTION)).retryWhen(retry);

        StepVerifier.create(flux)
                .expectNext(0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(4, SSLException.class));
    }

    @Test
    public void monoRetryWithExponentialBackoffWithJitter() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().jitterFactor(0.6).maxRetries(5).build()
                .retryWithExponentialBackoffAndJitter()
                .doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() -> Mono.error(SSL_EXCEPTION).retryWhen(retry))
                .expectSubscription()
                /*
                 * As we know for each ith retry - nextBackoff = minBackoffDelayMillis*pow(2,i), jitter high bound = jitterFactor*nextBackoff, jitter low bound = -jitterFactor*nextBackoff
                 * and effectiveBackoff = nextBackoff + random (jitter low bound, jitter high bound). So we should effectively pause for sum of (nextBackoff + jitter high bound) of all the retries.
                 */
                .thenAwait(Duration.ofMillis(100).plus(Duration.ofMillis(Double.valueOf(100 * 0.6).longValue())))
                .thenAwait(Duration.ofMillis(200).plus(Duration.ofMillis(Double.valueOf(200 * 0.6).longValue())))
                .thenAwait(Duration.ofMillis(400).plus(Duration.ofMillis(Double.valueOf(400 * 0.6).longValue())))
                .thenAwait(Duration.ofMillis(800).plus(Duration.ofMillis(Double.valueOf(800 * 0.6).longValue())))
                .thenAwait(Duration.ofMillis(1600).plus(Duration.ofMillis(Double.valueOf(1600 * 0.6).longValue())))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(5, SSLException.class));
    }

    @Test
    public void fluxRetryExponentialBackoffWithJitter() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().jitterFactor(0.6).maxRetries(4).build()
                .retryWithExponentialBackoffAndJitter()
                .doBeforeRetry(this.doOnRetryNew());

        final Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(SSL_EXCEPTION)).retryWhen(retry);

        StepVerifier.create(flux)
                .expectNext(0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertRetrySignals(Collections.nCopies(4, SSLException.class));
    }

    @Test
    public void fluxNonRetriableExceptions() {
        final RequestRetryConditions requestRetryConditions = RequestRetryConditions.builder().maxRetries(1).build();

        final Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IllegalStateException())).retryWhen(requestRetryConditions.retryWithExponentialBackoffAndJitter());
        StepVerifier.create(flux)
                .expectNext(0, 1)
                .verifyError(IllegalStateException.class);

        final Flux<Integer> retriable = Flux.concat(Flux.range(0, 2), Flux.error(SSL_EXCEPTION))
                .retryWhen(requestRetryConditions.retryWithExponentialBackoffAndJitter());
        StepVerifier.create(retriable)
                .expectNext(0, 1, 0, 1)
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();
    }

    @Test
    public void shouldTimeoutRetryWithVirtualTime() {
        final RetryBackoffSpec retry = RequestRetryConditions.builder()
                .maxRetries(5)
                .build()
                .retryWithExponentialBackoffAndJitter()
                .doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() ->
                Mono.<String>error(SSL_EXCEPTION).retryWhen(retry))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(Integer.MAX_VALUE))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();
    }

    @SuppressWarnings("deprecation")
    @DataProvider
    private Object[][] getException() {
        return new Object[][] {
                { new WebClientResponseException(HttpStatus.BAD_GATEWAY.value(), HttpStatus.BAD_GATEWAY.getReasonPhrase(),
                        null, "Bad Gateway".getBytes(), Charset.defaultCharset()), },
                { new WebClientResponseException(HttpStatus.GATEWAY_TIMEOUT.value(), HttpStatus.GATEWAY_TIMEOUT.getReasonPhrase(),
                        null, "Gateway Timeout".getBytes(), Charset.defaultCharset()), },
                { new WebClientResponseException(HttpStatus.SERVICE_UNAVAILABLE.value(), HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                        null, "Service Unavailable".getBytes(), Charset.defaultCharset()), },
                { new ConnectTimeoutException("Something went wrong") },
                { PrematureCloseException.BEFORE_RESPONSE },
                { PrematureCloseException.DURING_RESPONSE },
                { PrematureCloseException.BEFORE_RESPONSE_SENDING_REQUEST },
                { ReadTimeoutException.INSTANCE },
                { aConnectionResetException() },
                { new IOException("Broken pipe") },
                { new IOException("Connection reset by peer") },
                { new SocketException("Broken pipe") },
                { new SocketException("Connection reset by peer") },

        };
    }

    @Test(dataProvider = "getException")
    public void retriesForHttpStatusCodes(final Throwable exp) {
        final RetryBackoffSpec retry = RequestRetryConditions.builder().maxRetries(5).build()
                .retryWithExponentialBackoffAndJitter().doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() -> Mono.error(exp).retryWhen(retry))
                .expectSubscription()
                /*
                 * As we know for each ith retry - nextBackoff = minBackoffDelayMillis*pow(2,i), jitter high bound = jitterFactor*nextBackoff, jitter low bound = -jitterFactor*nextBackoff
                 * and effectiveBackoff = nextBackoff + random (jitter low bound, jitter high bound). So we should effectively pause for nextBackoff + jitter high bound of all the retries.
                 */
                .thenAwait(Duration.ofMillis(100).plus(Duration.ofMillis(Double.valueOf(100 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(200).plus(Duration.ofMillis(Double.valueOf(200 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(400).plus(Duration.ofMillis(Double.valueOf(400 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(800).plus(Duration.ofMillis(Double.valueOf(800 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(1600).plus(Duration.ofMillis(Double.valueOf(1600 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .verifyErrorMatches(e -> Exceptions.isRetryExhausted(e) && exp.getClass().isInstance(e.getCause()));

        assertRetrySignals(Collections.nCopies(5, exp.getClass()));
    }

    @Test
    public void fluxRetryWithExponentialBackoffWithJitterAndPredicate() {
        final Retry retry = RequestRetryConditions.builder().maxRetries(4).retryPredicate(AbortedException::isConnectionReset).build()
                .retryWithExponentialBackoffJitterAndPredicate().doBeforeRetry(this.doOnRetryNew());

        final Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(aConnectionResetException())).retryWhen(retry);

        StepVerifier.create(flux)
                .expectNext(0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
                .verifyErrorMatches(e -> Exceptions.isRetryExhausted(e) && AbortedException.class.isInstance(e.getCause()));

        assertRetrySignals(Collections.nCopies(4, AbortedException.class));
    }

    @Test
    public void monoRetryWithExponentialBackoffWithJitterAndPredicate() {
        final Retry retry = RequestRetryConditions.builder().maxRetries(4).retryPredicate(AbortedException::isConnectionReset).build()
                .retryWithExponentialBackoffJitterAndPredicate().doBeforeRetry(this.doOnRetryNew());

        StepVerifier.withVirtualTime(() -> Mono.error(aConnectionResetException()).retryWhen(retry))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100).plus(Duration.ofMillis(Double.valueOf(100 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(200).plus(Duration.ofMillis(Double.valueOf(200 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(400).plus(Duration.ofMillis(Double.valueOf(400 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(800).plus(Duration.ofMillis(Double.valueOf(800 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .thenAwait(Duration.ofMillis(1600).plus(Duration.ofMillis(Double.valueOf(1600 * RequestRetryConditions.DEFAULT_JITTER_FACTOR).longValue())))
                .verifyErrorMatches(e -> Exceptions.isRetryExhausted(e) && AbortedException.class.isInstance(e.getCause()));

        assertRetrySignals(Collections.nCopies(4, AbortedException.class));
    }

    @DataProvider
    private Object[][] noRetryWithExponentialBackoffJitterAndPredicateData() {
        final Predicate<? super Throwable> predicate1 = AbortedException::isConnectionReset;
        final Predicate<? super Throwable> predicate2 = exception -> StringUtils.isBlank(exception.getMessage());
        final Predicate<? super Throwable> predicate3 = exception -> Collections.singleton("retry").stream()
                .anyMatch(str -> StringUtils.containsIgnoreCase(exception.getMessage().trim(), str.trim()));

        return new Object[][] {
                { predicate1, new RuntimeException(THROWABLE_MSG), },
                { predicate2, new RuntimeException("random"), },
                { predicate2.negate(), new RuntimeException(""), },
                { predicate2.negate(), new RuntimeException("  "), },
                { predicate3, new RuntimeException("random"), },
                { predicate3.negate(), new RuntimeException("ReTry"), },
                { predicate3.negate(), new RuntimeException(" retry "), },
                { predicate3.negate(), new RuntimeException(" contains retry "), },
        };
    }

    @Test(dataProvider = "noRetryWithExponentialBackoffJitterAndPredicateData")
    public void noRetryWithExponentialBackoffWithJitterAndPredicateWhenInvalidPredicate(final Predicate<? super Throwable> predicate, final Throwable actualException) {
        //note effective predicate is - IS_RETRIABLE_EXCEPTION.or(predicate)
        final Retry retry = RequestRetryConditions.builder().maxRetries(4).retryPredicate(predicate).build()
                .retryWithExponentialBackoffJitterAndPredicate().doBeforeRetry(this.doOnRetryNew());

        StepVerifier.create(Mono.error(actualException).retryWhen(retry))
                .expectSubscription()
                .verifyError(actualException.getClass());

        assertRetrySignals(Collections.nCopies(0, null));
    }

    @Test
    public void noRetryWithAnyPredicates() {
        final String result = Mono.just("No Retry")
                .retryWhen(RequestRetryConditions.builder().build().retryWithExponentialBackoffJitterAndPredicate().doBeforeRetry(this.doOnRetryNew())).block();

        assertThat(result, is("No Retry"));
        assertThat(this.retrySignals, empty());
    }

    @DataProvider
    private Object[][] retryConditions() {
        return new Object[][] {
                { RequestRetryConditions.builder().build().retryWithExponentialBackoff() },
                { RequestRetryConditions.builder().build().retryWithExponentialBackoffAndJitter() },
                { RequestRetryConditions.builder().build().retryWithFixedBackoff() },
                { RequestRetryConditions.builder().build().retryWithNoBackoff() },
        };
    }

    @Test(dataProvider = "retryConditions",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "^Retries exhausted: 5/5$")
    public void monoRetryNoBackoffThrowsRetryExhaustedException(final RetryBackoffSpec rbs) {
        final List<String> order = new CopyOnWriteArrayList<>();

        try {
            final AtomicInteger count = new AtomicInteger();
            final RetryBackoffSpec retry = rbs
                    .doBeforeRetry(s -> order.add("BeforeRetry[" + count.incrementAndGet() + "]: " + s));

            Mono.error(aConnectionResetException())
                    .retryWhen(retry)
                    .block();
        } catch (final Exception e) {
            assertThat(Exceptions.isRetryExhausted(e), is(true));
            assertThat(order, hasSize(5));
            assertThat(order, hasItems(
                    "BeforeRetry[1]: attempt #1 (1 in a row), last failure={reactor.netty.channel.AbortedException: Connection has been closed BEFORE send operation}",
                    "BeforeRetry[2]: attempt #2 (2 in a row), last failure={reactor.netty.channel.AbortedException: Connection has been closed BEFORE send operation}",
                    "BeforeRetry[3]: attempt #3 (3 in a row), last failure={reactor.netty.channel.AbortedException: Connection has been closed BEFORE send operation}",
                    "BeforeRetry[4]: attempt #4 (4 in a row), last failure={reactor.netty.channel.AbortedException: Connection has been closed BEFORE send operation}",
                    "BeforeRetry[5]: attempt #5 (5 in a row), last failure={reactor.netty.channel.AbortedException: Connection has been closed BEFORE send operation}"));
            throw e;
        }
    }

    private void assertRetrySignals(final List<Class<? extends Throwable>> exceptions) {
        assertThat(this.retrySignals.size(), is(exceptions.size()));
        int index = 0;
        for (reactor.util.retry.Retry.RetrySignal retrySignal : this.retrySignals) {
            assertThat(retrySignal.totalRetries(), is((long)(index)));
            assertThat(retrySignal.failure(), instanceOf(exceptions.get(index)));
            index++;
        }
    }

    private Consumer<Retry.RetrySignal> doOnRetryNew() {
        return context -> this.retrySignals.add(context);
    }

    private static AbortedException aConnectionResetException() {
        return AbortedException.beforeSend();
    }
}
