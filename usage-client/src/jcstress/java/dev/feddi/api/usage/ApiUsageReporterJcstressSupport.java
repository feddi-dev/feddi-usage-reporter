package dev.feddi.api.usage;

import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.http.ReactiveHttpResponse;
import dev.feddi.api.usage.v1.IngestUsageRequest;
import dev.feddi.api.usage.v1.RegisterOperationsRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

final class ApiUsageReporterJcstressSupport {

    static final int REPORTS_PER_ACTOR = 1_000;
    static final ApiUsageInvocation INVOCATION = invocation();

    private ApiUsageReporterJcstressSupport() {
    }

    static ApiUsageReporter reporter(CountingReactiveHttpClient httpClient, int maxBatchSize, int maxQueueSize) {
        return ApiUsageReporter.builder(httpClient)
                .feddiGraphVariantKey("fddi_test_key")
                .autoStart(false)
                .flushInterval(Duration.ofDays(1))
                .maxBatchSize(maxBatchSize)
                .maxQueueSize(maxQueueSize)
                .randomSupplier(() -> 0.0)
                .scheduler(new NoopReporterScheduler())
                .build();
    }

    static void flush(ApiUsageReporter reporter) {
        reporter.flushNow().block(Duration.ofSeconds(5));
    }

    static void close(ApiUsageReporter reporter) {
        reporter.closeAsync().block(Duration.ofSeconds(5));
    }

    private static ApiUsageInvocation invocation() {
        return ApiUsageInvocation.builder()
                .document(Parser.parse("query GetUser { user { id name } }"))
                .operationName("GetUser")
                .schema(schema())
                .durationNanos(1_000)
                .clientName("jcstress")
                .clientVersion("1.0.0")
                .timestamp(Instant.parse("2026-05-08T00:00:00Z"))
                .build();
    }

    private static GraphQLSchema schema() {
        var registry = new SchemaParser().parse("""
                type Query {
                  user: User
                }

                type User {
                  id: ID!
                  name: String!
                }
                """);
        return new SchemaGenerator().makeExecutableSchema(registry, RuntimeWiring.newRuntimeWiring().build());
    }

    static final class CountingReactiveHttpClient implements ReactiveHttpClient {

        private final AtomicInteger acceptedCount = new AtomicInteger(0);

        @Override
        public Mono<ReactiveHttpResponse> exchange(ReactiveHttpRequest request) {
            return request.body()
                    .map(body -> {
                        int accepted;
                        if (request.uri().getPath().endsWith("/usage")) {
                            accepted = parseUsageRequest(body).getEventsCount();
                            acceptedCount.addAndGet(accepted);
                        } else {
                            accepted = parseOperationRequest(body).getOperationsCount();
                        }
                        return new ReactiveHttpResponse(
                                200,
                                Map.of(),
                                Mono.just(UsageReportResponse.newBuilder()
                                        .setAccepted(accepted)
                                        .build()
                                        .toByteArray())
                        );
                    });
        }

        int acceptedCount() {
            return acceptedCount.get();
        }

        private static RegisterOperationsRequest parseOperationRequest(byte[] body) {
            try {
                return RegisterOperationsRequest.parseFrom(gunzip(body));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private static IngestUsageRequest parseUsageRequest(byte[] body) {
            try {
                return IngestUsageRequest.parseFrom(gunzip(body));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private static byte[] gunzip(byte[] body) {
            try {
                try (var input = new GZIPInputStream(new ByteArrayInputStream(body));
                     var output = new ByteArrayOutputStream()) {
                    input.transferTo(output);
                    return output.toByteArray();
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }

    private static final class NoopReporterScheduler implements ReporterScheduler {

        @Override
        public void execute(Runnable task) {
        }

        @Override
        public Cancellable scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period) {
            return () -> {};
        }

        @Override
        public void close() {
        }
    }
}
