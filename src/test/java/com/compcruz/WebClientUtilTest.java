package com.compcruz;

import reactor.core.publisher.Mono;

import org.junit.Test;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class WebClientUtilTest {

    @Test
    public void getWebClient() {
        WebClient webClient = WebClientUtil.getWebClient();
        assertThat(webClient, is(notNullValue()));

        Mono<Result> result = webClient.get()
                .uri("https://postman-echo.com/status/200")
                .retrieve()
                .bodyToMono(Result.class);

        assertThat(result.block(), is(Result.builder().status(200).build()));
    }


}

@AllArgsConstructor
@Data
@Builder
@NoArgsConstructor
class Result {
    int status;
}
