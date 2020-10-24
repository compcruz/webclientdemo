package com.compcruz;

import reactor.netty.channel.AbortedException;
import reactor.netty.http.client.PrematureCloseException;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.Set;
import java.util.function.Predicate;

import javax.net.ssl.SSLException;

import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;


import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestRetryConditions {

    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final long FIXED_DELAY_MILLIS = 100;
    public static final long MIN_BACKOFF_DELAY_MILLIS = 100;
    public static final long MAX_BACKOFF_DELAY_MILLIS = 3000;
    public static final double ZERO_JITTER_FACTOR = 0d;
    public static final double DEFAULT_JITTER_FACTOR = 0.5d;
    public static final Predicate<? super Throwable> DEFAULT_PREDICATE = x -> false;
    public static final Set<HttpStatus> RETRY_ON_STATUS_CODES = Set.of(
            HttpStatus.BAD_GATEWAY,
            HttpStatus.GATEWAY_TIMEOUT,
            HttpStatus.REQUEST_TIMEOUT,
            HttpStatus.SERVICE_UNAVAILABLE);

    // Retriable exceptions
    private static final Predicate<Throwable> IS_SSL_EXCEPTION = throwable -> SSLException.class.isAssignableFrom(throwable.getClass());
    private static final Predicate<Throwable> IS_CONNECTION_RESET = AbortedException::isConnectionReset;
    private static final Predicate<Throwable> IS_CONNECT_TIMEOUT_EXCEPTION = throwable -> ConnectTimeoutException.class.isAssignableFrom(throwable.getClass());
    private static final Predicate<Throwable> IS_READ_TIMEOUT_EXCEPTION = throwable -> ReadTimeoutException.class.isAssignableFrom(throwable.getClass());
    private static final Predicate<Throwable> IS_PREMATURE_CLOSE_EXCEPTION = throwable -> PrematureCloseException.class.isAssignableFrom(throwable.getClass());
    private static final Predicate<Throwable> IS_RETRIABLE_WEB_CLIENT_EXCEPTION = throwable ->
            WebClientResponseException.class.isAssignableFrom(throwable.getClass())
                    && RETRY_ON_STATUS_CODES.contains(((WebClientResponseException)throwable).getStatusCode());

    private static final Predicate<Throwable> IS_RETRIABLE_EXCEPTION = IS_SSL_EXCEPTION
            .or(IS_CONNECTION_RESET)
            .or(IS_CONNECT_TIMEOUT_EXCEPTION)
            .or(IS_READ_TIMEOUT_EXCEPTION)
            .or(IS_PREMATURE_CLOSE_EXCEPTION)
            .or(IS_RETRIABLE_WEB_CLIENT_EXCEPTION);

    @Builder.Default
    private int maxRetries = DEFAULT_MAX_RETRIES;

    @Builder.Default
    private long fixedDelayMillis = FIXED_DELAY_MILLIS;

    @Builder.Default
    private long minBackoffDelayMillis = MIN_BACKOFF_DELAY_MILLIS;

    @Builder.Default
    private long maxBackoffDelayMillis = MAX_BACKOFF_DELAY_MILLIS;

    @Builder.Default
    private Predicate<? super Throwable> retryPredicate = DEFAULT_PREDICATE;

    @Builder.Default
    private double jitterFactor = DEFAULT_JITTER_FACTOR;

    public RetryBackoffSpec retryWithNoBackoff() {
        return Retry.fixedDelay(this.maxRetries, Duration.ZERO)
                .filter(IS_RETRIABLE_EXCEPTION);
    }

    public RetryBackoffSpec retryWithFixedBackoff() {
        return Retry.fixedDelay(this.maxRetries, Duration.ofMillis(this.fixedDelayMillis))
                .filter(IS_RETRIABLE_EXCEPTION);
    }

    public RetryBackoffSpec retryWithExponentialBackoff() {
        return Retry.backoff(this.maxRetries, Duration.ofMillis(this.minBackoffDelayMillis))
                .maxBackoff(Duration.ofMillis(this.maxBackoffDelayMillis))
                .jitter(ZERO_JITTER_FACTOR)
                .filter(IS_RETRIABLE_EXCEPTION);
    }

    public RetryBackoffSpec retryWithExponentialBackoffAndJitter() {
        return Retry.backoff(this.maxRetries, Duration.ofMillis(this.minBackoffDelayMillis))
                .maxBackoff(Duration.ofMillis(this.maxBackoffDelayMillis))
                .jitter(this.jitterFactor)
                .filter(IS_RETRIABLE_EXCEPTION);
    }

    public RetryBackoffSpec retryWithExponentialBackoffJitterAndPredicate() {
        return Retry.backoff(this.maxRetries, Duration.ofMillis(this.minBackoffDelayMillis))
                .maxBackoff(Duration.ofMillis(this.maxBackoffDelayMillis))
                .jitter(this.jitterFactor)
                .filter(IS_RETRIABLE_EXCEPTION.or(this.retryPredicate));
    }
}
