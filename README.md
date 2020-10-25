# Utility classes _WebClientComposer_ and _RequestRetryConditions_
**`WebClientComposer`** - Provides WebClient instance with options to specify custom - connection pool, request/response filters, connection and read timeouts etc.

**`RequestRetryConditions`** - Provides Retry strategy with configurable features.

~~~~
Also, includes CircuitBreakerDemo showing how to decorate original function to add circuit breaker.

With following sample CircuitBreakerConfig which moves to OPEN state after crossing 50% failure threshold with minimum 3 calls,
 then after 20ms to HALF OPEN (between OPEN and HALF OPEN decorated function call throws CallNotPermittedException), 
 and finally after next 3 success calls circuit breaker goes back to CLOSED state - 

CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .recordException(throwable -> throwable instanceof WebClientResponseException || throwable instanceof IllegalStateException)
                .slidingWindow(5, 3, CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .permittedNumberOfCallsInHalfOpenState(5)
                .waitDurationInOpenState(Duration.ofMillis(20))
                .enableAutomaticTransitionFromOpenToHalfOpen()
                .build();
Sample Execution logs -                 
21:31:07.938 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 1
21:31:07.952 [main] INFO  com.compcruz.CircuitBreakerDemo - IllegalStateException
21:31:07.953 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 2
21:31:07.953 [main] INFO  com.compcruz.CircuitBreakerDemo - IllegalStateException
21:31:07.953 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 3
21:31:07.983 [main] INFO  com.compcruz.CircuitBreakerDemo - CircuitBreaker.StateTransition =State transition from CLOSED to OPEN
21:31:07.983 [main] INFO  com.compcruz.CircuitBreakerDemo - IllegalStateException
21:31:07.984 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 4
21:31:07.985 [main] INFO  com.compcruz.CircuitBreakerDemo - CallNotPermittedException
21:31:07.985 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 5
21:31:07.987 [main] INFO  com.compcruz.CircuitBreakerDemo - CircuitBreaker.StateTransition =State transition from OPEN to HALF_OPEN
21:31:07.987 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 6
21:31:07.988 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 7
21:31:07.990 [main] INFO  com.compcruz.CircuitBreakerDemo - CircuitBreaker.StateTransition =State transition from HALF_OPEN to CLOSED
21:31:07.990 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 8
21:31:07.990 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 9
21:31:07.991 [main] INFO  com.compcruz.CircuitBreakerDemo - Call - 10
~~~~