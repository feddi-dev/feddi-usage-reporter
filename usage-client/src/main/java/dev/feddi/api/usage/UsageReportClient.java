package dev.feddi.api.usage;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.v1.UsageReportRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class UsageReportClient {

    public static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
    public static final String DEFAULT_USAGE_PROTO_PATH = "/api/usage-proto";

    private final ReactiveHttpClient httpClient;
    private final URI endpointUri;
    private final String bearerToken;

    private UsageReportClient(Builder builder) {
        this.httpClient = Objects.requireNonNull(builder.httpClient, "httpClient");
        this.endpointUri = builder.endpointUri != null
                ? builder.endpointUri
                : resolveEndpoint(Objects.requireNonNull(builder.baseUri, "baseUri"));
        this.bearerToken = requireNonBlank(builder.bearerToken, "bearerToken");
    }

    public static Builder builder(ReactiveHttpClient httpClient) {
        return new Builder(httpClient);
    }

    public Mono<UsageReportResponse> report(UsageReportRequest request) {
        Objects.requireNonNull(request, "request");

        var httpRequest = new ReactiveHttpRequest(
                "POST",
                endpointUri,
                Map.of(
                        "Authorization", List.of("Bearer " + bearerToken),
                        "Content-Type", List.of(PROTOBUF_CONTENT_TYPE),
                        "Accept", List.of(PROTOBUF_CONTENT_TYPE)
                ),
                Mono.just(request.toByteArray())
        );

        return httpClient.exchange(httpRequest)
                .switchIfEmpty(Mono.error(new UsageReportClientException("HTTP client completed without a response")))
                .flatMap(response -> response.body()
                        .defaultIfEmpty(new byte[0])
                        .flatMap(body -> parseResponse(response.statusCode(), body)));
    }

    private static Mono<UsageReportResponse> parseResponse(int statusCode, byte[] body) {
        if (statusCode < 200 || statusCode >= 300) {
            return Mono.error(new UsageReportClientException(
                    "Usage report request failed with HTTP status " + statusCode,
                    statusCode
            ));
        }

        try {
            return Mono.just(UsageReportResponse.parseFrom(body));
        } catch (InvalidProtocolBufferException e) {
            return Mono.error(new UsageReportClientException("Usage report response was not valid protobuf", e));
        }
    }

    private static URI resolveEndpoint(URI baseUri) {
        String base = baseUri.toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return URI.create(base + DEFAULT_USAGE_PROTO_PATH);
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    public static final class Builder {

        private final ReactiveHttpClient httpClient;
        private URI baseUri;
        private URI endpointUri;
        private String bearerToken;

        private Builder(ReactiveHttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        }

        public Builder baseUri(URI baseUri) {
            this.baseUri = Objects.requireNonNull(baseUri, "baseUri");
            return this;
        }

        public Builder endpointUri(URI endpointUri) {
            this.endpointUri = Objects.requireNonNull(endpointUri, "endpointUri");
            return this;
        }

        public Builder bearerToken(String bearerToken) {
            this.bearerToken = bearerToken;
            return this;
        }

        public UsageReportClient build() {
            if (baseUri == null && endpointUri == null) {
                throw new IllegalStateException("baseUri or endpointUri must be configured");
            }
            return new UsageReportClient(this);
        }
    }
}
