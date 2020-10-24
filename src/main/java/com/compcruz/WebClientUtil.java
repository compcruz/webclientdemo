package com.compcruz;

import org.springframework.web.reactive.function.client.WebClient;

public final class WebClientUtil {

    public static WebClient getWebClient() {
        return WebClient.create();
    }

    public static WebClient getWebClient(String baseUrl) {
        return WebClient.create(baseUrl);
    }
}
