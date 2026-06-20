package dev.feddi.api.usage.http;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ReactiveHttpResponse(
        int statusCode,
        Map<String, List<String>> headers,
        Mono<byte[]> body
) {

    public ReactiveHttpResponse {
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(body, "body");
        headers = Map.copyOf(headers.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue())
                )));
    }
}
