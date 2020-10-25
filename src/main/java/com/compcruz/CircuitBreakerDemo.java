package com.compcruz;

import java.time.Duration;
import java.util.function.Function;

import org.springframework.web.reactive.function.client.WebClientResponseException;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import io.github.resilience4j.core.EventConsumer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CircuitBreakerDemo {


    // Registry
    final static CircuitBreakerRegistry getCircuitBreakerRegistry() {
        return CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults());
    }

    // Config
    final static CircuitBreakerConfig getCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .recordException(throwable -> throwable instanceof WebClientResponseException || throwable instanceof IllegalStateException)
                .slidingWindow(5, 3, CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .permittedNumberOfCallsInHalfOpenState(5)
                .waitDurationInOpenState(Duration.ofMillis(20))
                .enableAutomaticTransitionFromOpenToHalfOpen()
                .build();
    }

    // StateTransition Event Consumer
    final static EventConsumer<CircuitBreakerOnStateTransitionEvent> STATE_TRANSITION_EVENT_CONSUMER = event -> {
        log.info("CircuitBreaker.StateTransition =" + event.getStateTransition());
    };



    public static void main(String [] args) {

        // Circuit Breaker
        final CircuitBreaker circuitBreaker = getCircuitBreakerRegistry().circuitBreaker("CircuitBreakerDemo", getCircuitBreakerConfig());

        circuitBreaker.getEventPublisher().onStateTransition(STATE_TRANSITION_EVENT_CONSUMER);


        final Function<Integer, Integer> originalFunction = i ->  {
            if(i <= 3) {
                throw new IllegalStateException("something bad happened");
            }
            return i;
        };

        final Function<Integer, Integer> decorated = CircuitBreaker
                .decorateFunction(circuitBreaker, originalFunction);

        for (int i = 1; i <= 10; i++) {
            try {
                log.info("Call - " + i);
                decorated.apply(i);
            } catch (Exception e) {
                log.info(e.getClass().getSimpleName());
            }
        }

    }
}
