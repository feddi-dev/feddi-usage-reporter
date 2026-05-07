package dev.feddi.api.usage;

import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.http.ReactiveHttpResponse;
import dev.feddi.api.usage.v1.UsageRecord;
import dev.feddi.api.usage.v1.UsageReportRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UsageReportClientTest {

    @Test
    void report_sendsProtobufRequestToUsageProtoEndpoint() {
        var capturedRequest = new AtomicReference<ReactiveHttpRequest>();
        ReactiveHttpClient httpClient = request -> {
            capturedRequest.set(request);
            return Mono.just(new ReactiveHttpResponse(
                    200,
                    Map.of(),
                    Mono.just(UsageReportResponse.newBuilder().setAccepted(1).build().toByteArray())
            ));
        };
        var client = UsageReportClient.builder(httpClient)
                .baseUri(URI.create("https://api.example.test"))
                .bearerToken("fddi_test_key")
                .build();
        var request = UsageReportRequest.newBuilder()
                .addRecords(UsageRecord.newBuilder()
                        .setOperationName("GetUser")
                        .setOperationType("QUERY")
                        .setOperationHash("abc123")
                        .addFieldCoordinates("Query.user")
                        .setDurationNanos(1_000_000)
                        .build())
                .build();

        StepVerifier.create(client.report(request))
                .assertNext(response -> assertEquals(1, response.getAccepted()))
                .verifyComplete();

        var httpRequest = capturedRequest.get();
        assertNotNull(httpRequest);
        assertEquals("POST", httpRequest.method());
        assertEquals(URI.create("https://api.example.test/api/usage-proto"), httpRequest.uri());
        assertEquals("Bearer fddi_test_key", httpRequest.headers().get("Authorization").getFirst());
        assertEquals(UsageReportClient.PROTOBUF_CONTENT_TYPE, httpRequest.headers().get("Content-Type").getFirst());
        assertEquals(UsageReportClient.PROTOBUF_CONTENT_TYPE, httpRequest.headers().get("Accept").getFirst());

        StepVerifier.create(httpRequest.body())
                .assertNext(body -> {
                    try {
                        assertEquals(request, UsageReportRequest.parseFrom(body));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void report_usesConfiguredEndpointUri() {
        var capturedRequest = new AtomicReference<ReactiveHttpRequest>();
        ReactiveHttpClient httpClient = request -> {
            capturedRequest.set(request);
            return Mono.just(new ReactiveHttpResponse(
                    200,
                    Map.of(),
                    Mono.just(UsageReportResponse.newBuilder().build().toByteArray())
            ));
        };

        var client = UsageReportClient.builder(httpClient)
                .endpointUri(URI.create("https://api.example.test/custom"))
                .bearerToken("fddi_test_key")
                .build();

        StepVerifier.create(client.report(UsageReportRequest.newBuilder().build()))
                .expectNextCount(1)
                .verifyComplete();

        assertEquals(URI.create("https://api.example.test/custom"), capturedRequest.get().uri());
    }

    @Test
    void report_surfacesNonSuccessStatus() {
        ReactiveHttpClient httpClient = request -> Mono.just(new ReactiveHttpResponse(403, Map.of(), Mono.just(new byte[0])));
        var client = UsageReportClient.builder(httpClient)
                .baseUri(URI.create("https://api.example.test"))
                .bearerToken("fddi_test_key")
                .build();

        StepVerifier.create(client.report(UsageReportRequest.newBuilder().build()))
                .expectErrorSatisfies(error -> {
                    var usageError = (UsageReportClientException) error;
                    assertEquals(403, usageError.statusCode());
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
        var client = UsageReportClient.builder(httpClient)
                .baseUri(URI.create("https://api.example.test"))
                .bearerToken("fddi_test_key")
                .build();

        StepVerifier.create(client.report(UsageReportRequest.newBuilder().build()))
                .expectError(UsageReportClientException.class)
                .verify();
    }
}
