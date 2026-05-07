package dev.feddi.api.usage.http;

import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ReactiveHttpRequest(
        String method,
        URI uri,
        Map<String, List<String>> headers,
        Mono<byte[]> body
) {

    public ReactiveHttpRequest {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(body, "body");

        method = method.trim().toUpperCase();
        if (method.isEmpty()) {
            throw new IllegalArgumentException("method must not be blank");
        }
        headers = copyHeaders(headers);
    }

    private static Map<String, List<String>> copyHeaders(Map<String, List<String>> headers) {
        return Map.copyOf(headers.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue())
                )));
    }
}
