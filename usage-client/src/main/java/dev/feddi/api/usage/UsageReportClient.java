package dev.feddi.api.usage;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.v1.GetKnownOperationHashesRequest;
import dev.feddi.api.usage.v1.IngestUsageRequest;
import dev.feddi.api.usage.v1.KnownOperationHashesResponse;
import dev.feddi.api.usage.v1.RegisterOperationsRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

final class UsageReportClient {

    static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
    static final String OPERATIONS_PROTO_PATH = "/api/usage-proto/operations";
    static final String KNOWN_OPERATION_HASHES_PROTO_PATH = "/api/usage-proto/known-operation-hashes";
    static final String USAGE_PROTO_PATH = "/api/usage-proto/usage";
    static final URI DEFAULT_PLATFORM_HOST = URI.create("https://feddi.dev");

    private final ReactiveHttpClient httpClient;
    private final URI operationsEndpointUri;
    private final URI knownOperationHashesEndpointUri;
    private final URI usageEndpointUri;
    private final String bearerToken;

    UsageReportClient(ReactiveHttpClient httpClient, URI host, String bearerToken) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.operationsEndpointUri = resolveEndpoint(Objects.requireNonNull(host, "host"), OPERATIONS_PROTO_PATH);
        this.knownOperationHashesEndpointUri = resolveEndpoint(host, KNOWN_OPERATION_HASHES_PROTO_PATH);
        this.usageEndpointUri = resolveEndpoint(host, USAGE_PROTO_PATH);
        this.bearerToken = requireNonBlank(bearerToken, "bearerToken");
    }

    Mono<UsageReportResponse> registerOperations(RegisterOperationsRequest request) {
        Objects.requireNonNull(request, "request");
        return send(operationsEndpointUri, request.toByteArray());
    }

    Mono<KnownOperationHashesResponse> getKnownOperationHashes() {
        var request = GetKnownOperationHashesRequest.newBuilder().build();
        return send(
                knownOperationHashesEndpointUri,
                request.toByteArray(),
                KnownOperationHashesResponse::parseFrom,
                "known operation hashes response"
        );
    }

    Mono<UsageReportResponse> report(IngestUsageRequest request) {
        Objects.requireNonNull(request, "request");
        return send(usageEndpointUri, request.toByteArray());
    }

    private Mono<UsageReportResponse> send(URI endpointUri, byte[] protobufBody) {
        return send(endpointUri, protobufBody, UsageReportResponse::parseFrom, "usage report response");
    }

    private <T> Mono<T> send(
            URI endpointUri,
            byte[] protobufBody,
            ProtobufParser<T> responseParser,
            String responseDescription
    ) {
        byte[] compressedBody;
        try {
            compressedBody = gzip(protobufBody);
        } catch (IOException e) {
            return Mono.error(new UsageReportClientException("Failed to gzip usage report request", e));
        }

        var httpRequest = new ReactiveHttpRequest("POST", endpointUri, headers(), Mono.just(compressedBody));

        return httpClient.exchange(httpRequest)
                .switchIfEmpty(Mono.error(new UsageReportClientException("HTTP client completed without a response")))
                .flatMap(response -> response.body()
                        .defaultIfEmpty(new byte[0])
                        .flatMap(body -> parseResponse(response.statusCode(), body, responseParser, responseDescription)));
    }

    private Map<String, List<String>> headers() {
        return Map.of(
                "Authorization", List.of("Bearer " + bearerToken),
                "Content-Type", List.of(PROTOBUF_CONTENT_TYPE),
                "Content-Encoding", List.of("gzip"),
                "Accept", List.of(PROTOBUF_CONTENT_TYPE)
        );
    }

    private static <T> Mono<T> parseResponse(
            int statusCode,
            byte[] body,
            ProtobufParser<T> responseParser,
            String responseDescription
    ) {
        if (statusCode < 200 || statusCode >= 300) {
            return Mono.error(new UsageReportClientException(
                    "Usage report request failed with HTTP status " + statusCode,
                    statusCode
            ));
        }

        try {
            return Mono.just(responseParser.parse(body));
        } catch (InvalidProtocolBufferException e) {
            return Mono.error(new UsageReportClientException(responseDescription + " was not valid protobuf", e));
        }
    }

    private static URI resolveEndpoint(URI baseUri, String path) {
        if (!baseUri.isAbsolute()) {
            throw new IllegalArgumentException("host must be an absolute URI");
        }
        if (baseUri.getPath() != null && !baseUri.getPath().isBlank() && !baseUri.getPath().equals("/")) {
            throw new IllegalArgumentException("host must not include a path");
        }
        if (baseUri.getQuery() != null || baseUri.getFragment() != null) {
            throw new IllegalArgumentException("host must not include a query or fragment");
        }
        String base = baseUri.toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return URI.create(base + path);
    }

    static byte[] gzip(byte[] body) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
            gzip.write(body);
        }
        return output.toByteArray();
    }

    private static String requireNonBlank(@Nullable String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private interface ProtobufParser<T> {
        T parse(byte[] body) throws InvalidProtocolBufferException;
    }
}
