package dev.feddi.api.usage;

import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.http.ReactiveHttpResponse;
import dev.feddi.api.usage.v1.GetKnownOperationHashesRequest;
import dev.feddi.api.usage.v1.IngestUsageRequest;
import dev.feddi.api.usage.v1.KnownOperationHashesResponse;
import dev.feddi.api.usage.v1.OperationDefinition;
import dev.feddi.api.usage.v1.RegisterOperationsRequest;
import dev.feddi.api.usage.v1.UsageEvent;
import dev.feddi.api.usage.v1.UsageReportResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UsageReportClientTest {

    @Test
    void registerOperations_sendsGzippedProtobufRequestToOperationsEndpoint() {
        var capturedRequest = new AtomicReference<ReactiveHttpRequest>();
        ReactiveHttpClient httpClient = successClient(capturedRequest, 1);
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );
        var request = RegisterOperationsRequest.newBuilder()
                .addOperations(OperationDefinition.newBuilder()
                        .setOperationHash("abc123")
                        .setOperationName("GetUser")
                        .setOperationType("QUERY")
                        .setCanonicalDocument("query GetUser { user { id } }")
                        .addFieldCoordinates("Query.user")
                        .build())
                .build();

        StepVerifier.create(client.registerOperations(request))
                .assertNext(response -> assertEquals(1, response.getAccepted()))
                .verifyComplete();

        var httpRequest = capturedRequest.get();
        assertCommonRequest(httpRequest, URI.create("https://api.example.test/api/usage-proto/operations"));
        StepVerifier.create(httpRequest.body())
                .assertNext(body -> {
                    try {
                        assertEquals(request, RegisterOperationsRequest.parseFrom(gunzip(body)));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void report_sendsGzippedProtobufRequestToUsageEndpoint() {
        var capturedRequest = new AtomicReference<ReactiveHttpRequest>();
        ReactiveHttpClient httpClient = successClient(capturedRequest, 1);
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );
        var request = IngestUsageRequest.newBuilder()
                .addEvents(UsageEvent.newBuilder()
                        .setOperationHash("abc123")
                        .setDurationNanos(1_000_000)
                        .build())
                .build();

        StepVerifier.create(client.report(request))
                .assertNext(response -> assertEquals(1, response.getAccepted()))
                .verifyComplete();

        var httpRequest = capturedRequest.get();
        assertCommonRequest(httpRequest, URI.create("https://api.example.test/api/usage-proto/usage"));
        StepVerifier.create(httpRequest.body())
                .assertNext(body -> {
                    try {
                        assertEquals(request, IngestUsageRequest.parseFrom(gunzip(body)));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void getKnownOperationHashes_sendsGzippedProtobufRequestToKnownHashesEndpoint() {
        var capturedRequest = new AtomicReference<ReactiveHttpRequest>();
        ReactiveHttpClient httpClient = request -> {
            capturedRequest.set(request);
            var response = KnownOperationHashesResponse.newBuilder()
                    .addOperationHashes("hash-a")
                    .addOperationHashes("hash-b")
                    .build();
            return Mono.just(new ReactiveHttpResponse(200, Map.of(), Mono.just(response.toByteArray())));
        };
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );

        StepVerifier.create(client.getKnownOperationHashes())
                .assertNext(response -> assertEquals(
                        java.util.List.of("hash-a", "hash-b"),
                        response.getOperationHashesList()
                ))
                .verifyComplete();

        var httpRequest = capturedRequest.get();
        assertCommonRequest(httpRequest, URI.create("https://api.example.test/api/usage-proto/known-operation-hashes"));
        StepVerifier.create(httpRequest.body())
                .assertNext(body -> {
                    try {
                        assertEquals(GetKnownOperationHashesRequest.getDefaultInstance(),
                                GetKnownOperationHashesRequest.parseFrom(gunzip(body)));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void report_trimsTrailingSlashFromConfiguredHost() {
        var capturedRequest = new AtomicReference<ReactiveHttpRequest>();
        ReactiveHttpClient httpClient = successClient(capturedRequest, 0);
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test/"),
                "fddi_test_key"
        );

        StepVerifier.create(client.report(IngestUsageRequest.newBuilder().build()))
                .expectNextCount(1)
                .verifyComplete();

        assertEquals(URI.create("https://api.example.test/api/usage-proto/usage"), capturedRequest.get().uri());
    }

    @Test
    void report_surfacesNonSuccessStatus() {
        ReactiveHttpClient httpClient = request -> Mono.just(new ReactiveHttpResponse(403, Map.of(), Mono.just(new byte[0])));
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );

        StepVerifier.create(client.report(IngestUsageRequest.newBuilder().build()))
                .expectErrorSatisfies(error -> {
                    var usageError = (UsageReportClientException) error;
                    assertEquals(403, usageError.statusCode());
                })
                .verify();
    }

    @Test
    void getKnownOperationHashes_surfacesNonSuccessStatus() {
        ReactiveHttpClient httpClient = request -> Mono.just(new ReactiveHttpResponse(503, Map.of(), Mono.just(new byte[0])));
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );

        StepVerifier.create(client.getKnownOperationHashes())
                .expectErrorSatisfies(error -> {
                    var usageError = (UsageReportClientException) error;
                    assertEquals(503, usageError.statusCode());
                })
                .verify();
    }

    @Test
    void report_surfacesInvalidProtobufResponse() {
        ReactiveHttpClient httpClient = request -> Mono.just(new ReactiveHttpResponse(
                200,
                Map.of(),
                Mono.just(new byte[]{1, 2, 3})
        ));
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );

        StepVerifier.create(client.report(IngestUsageRequest.newBuilder().build()))
                .expectError(UsageReportClientException.class)
                .verify();
    }

    @Test
    void getKnownOperationHashes_surfacesInvalidProtobufResponse() {
        ReactiveHttpClient httpClient = request -> Mono.just(new ReactiveHttpResponse(
                200,
                Map.of(),
                Mono.just(new byte[]{1, 2, 3})
        ));
        var client = new UsageReportClient(
                httpClient,
                URI.create("https://api.example.test"),
                "fddi_test_key"
        );

        StepVerifier.create(client.getKnownOperationHashes())
                .expectError(UsageReportClientException.class)
                .verify();
    }

    private static ReactiveHttpClient successClient(AtomicReference<ReactiveHttpRequest> capturedRequest, int accepted) {
        return request -> {
            capturedRequest.set(request);
            return Mono.just(new ReactiveHttpResponse(
                    200,
                    Map.of(),
                    Mono.just(UsageReportResponse.newBuilder().setAccepted(accepted).build().toByteArray())
            ));
        };
    }

    private static void assertCommonRequest(ReactiveHttpRequest httpRequest, URI expectedUri) {
        assertNotNull(httpRequest);
        assertEquals("POST", httpRequest.method());
        assertEquals(expectedUri, httpRequest.uri());
        assertEquals("Bearer fddi_test_key", httpRequest.headers().get("Authorization").getFirst());
        assertEquals(UsageReportClient.PROTOBUF_CONTENT_TYPE, httpRequest.headers().get("Content-Type").getFirst());
        assertEquals("gzip", httpRequest.headers().get("Content-Encoding").getFirst());
        assertEquals(UsageReportClient.PROTOBUF_CONTENT_TYPE, httpRequest.headers().get("Accept").getFirst());
    }

    private static byte[] gunzip(byte[] body) throws Exception {
        try (var input = new GZIPInputStream(new ByteArrayInputStream(body));
             var output = new ByteArrayOutputStream()) {
            input.transferTo(output);
            return output.toByteArray();
        }
    }
}
