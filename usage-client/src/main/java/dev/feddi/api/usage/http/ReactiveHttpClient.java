package dev.feddi.api.usage.http;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface ReactiveHttpClient {

    Mono<ReactiveHttpResponse> exchange(ReactiveHttpRequest request);
}
