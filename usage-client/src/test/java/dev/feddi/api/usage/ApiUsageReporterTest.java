package dev.feddi.api.usage;

import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.http.ReactiveHttpResponse;
import dev.feddi.api.usage.v1.IngestUsageRequest;
import dev.feddi.api.usage.v1.KnownOperationHashesResponse;
import dev.feddi.api.usage.v1.RegisterOperationsRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import graphql.language.Document;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.DoubleSupplier;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiUsageReporterTest {

    @Test
    void build_requiresFeddiGraphVariantKey() {
        var httpClient = new InMemoryReactiveHttpClient();

        assertThatThrownBy(() -> ApiUsageReporter.builder(httpClient)
                .autoStart(false)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("feddiGraphVariantKey must not be blank");
    }

    @Test
    void defaultsUseNoSamplingTwentyToFortySecondBatchWindowAndOneMbQueueSize() {
        assertThat(ApiUsageReporter.DEFAULT_BATCH_WINDOW_MIN).isEqualTo(Duration.ofSeconds(20));
        assertThat(ApiUsageReporter.DEFAULT_BATCH_WINDOW_MAX).isEqualTo(Duration.ofSeconds(40));
        assertThat(ApiUsageReporter.APPROX_COMPRESSED_USAGE_EVENT_BYTES).isEqualTo(45);
        assertThat(ApiUsageReporter.DEFAULT_MAX_QUEUE_SIZE).isEqualTo(22_222);
        assertThat(ApiUsageReporter.ABSOLUTE_MAX_QUEUE_SIZE).isEqualTo(44_444);
    }

    @Test
    void flushNow_isColdAndSendsQueuedUsageOnlyWhenSubscribed() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
        assertThat(httpClient.requests()).isEmpty();

        Mono<UsageReportResponse> flush = reporter.flushNow();

        assertThat(httpClient.requests()).isEmpty();

        StepVerifier.create(flush)
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(reporter.getPendingQueueSize()).isZero();
        assertThat(httpClient.requests()).hasSize(3);
    }

    @Test
    void flushNow_prefetchesKnownOperationHashesAndReturnsAcceptedZeroWhenQueueIsEmpty() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient).build();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isZero())
                .verifyComplete();

        assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
        assertThat(httpClient.operationRequests()).isEmpty();
        assertThat(httpClient.usageRequests()).isEmpty();
    }

    @Test
    void flushNow_sendsQueuedRecordsWithMetadataHeadersAndDefaultEndpoint() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = ApiUsageReporter.builder(httpClient)
                .feddiGraphVariantKey("fddi_test_key")
                .scheduler(new ManualReporterScheduler())
                .autoStart(false)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .randomSupplier(() -> 0.0)
                .build();

        assertThat(reporter.report(invocation("""
                query GetUser {
                  __typename
                  user {
                    id
                    ...UserFields
                    ... on User {
                      friend { name }
                    }
                  }
                }

                fragment UserFields on User {
                  name
                }
                """))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        var operationHttpRequest = httpClient.requestEndingWith("/operations");
        assertThat(operationHttpRequest.method()).isEqualTo("POST");
        assertThat(operationHttpRequest.uri()).isEqualTo(URI.create("https://feddi.dev/api/usage-proto/operations"));
        assertThat(operationHttpRequest.headers()).containsEntry("Authorization", List.of("Bearer fddi_test_key"));
        assertThat(operationHttpRequest.headers()).containsEntry("Content-Type", List.of("application/x-protobuf"));
        assertThat(operationHttpRequest.headers()).containsEntry("Content-Encoding", List.of("gzip"));
        assertThat(operationHttpRequest.headers()).containsEntry("Accept", List.of("application/x-protobuf"));

        var usageHttpRequest = httpClient.requestEndingWith("/usage");
        assertThat(usageHttpRequest.uri()).isEqualTo(URI.create("https://feddi.dev/api/usage-proto/usage"));
        assertThat(usageHttpRequest.headers()).containsEntry("Content-Encoding", List.of("gzip"));

        var operation = httpClient.operationRequest(0).getOperations(0);
        assertThat(operation.getOperationName()).isEqualTo("GetUser");
        assertThat(operation.getOperationType()).isEqualTo("QUERY");
        assertThat(operation.getOperationHash()).isNotBlank();
        assertThat(operation.getCanonicalDocument()).isNotBlank();
        assertThat(operation.getFieldCoordinatesList()).containsExactly(
                "Query.user",
                "User.friend",
                "User.id",
                "User.name"
        );
        var event = httpClient.usageRequest(0).getEvents(0);
        assertThat(event.getOperationHash()).isEqualTo(operation.getOperationHash());
        assertThat(event.getDurationNanos()).isEqualTo(1_500_000);
        assertThat(event.getClientName()).isEqualTo("web-app");
        assertThat(event.getClientVersion()).isEqualTo("1.2.0");
        assertThat(event.getTimestamp().getSeconds())
                .isEqualTo(Instant.parse("2026-03-22T10:00:00Z").getEpochSecond());
    }

    @Test
    void flushNow_sendsInputUsageCoordinates() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation(
                "GetUser",
                """
                query GetUser($filter: UserFilter) {
                  user(id: "1", filter: $filter) {
                    id
                  }
                }
                """,
                Map.of("filter", Map.of(
                        "name", "Ada",
                        "friend", Map.of("name", "Grace")
                ))
        ))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        var operation = httpClient.operationRequest(0).getOperations(0);
        assertThat(operation.getInputUsageCoordinatesList())
                .extracting(coordinate -> coordinate.getKind() + ":" + coordinate.getCoordinate())
                .containsExactlyInAnyOrder(
                        "FIELD_ARGUMENT:Query.user(id:)",
                        "FIELD_ARGUMENT:Query.user(filter:)",
                        "INPUT_OBJECT_FIELD:UserFilter.name",
                        "INPUT_OBJECT_FIELD:UserFilter.friend"
                );
    }

    @Test
    void report_dropsWhenQueueIsFull() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .maxQueueSize(1)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();

        assertThat(reporter.getDroppedCount()).isEqualTo(1);
        assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
        assertThat(httpClient.requests()).isEmpty();
    }

    @Test
    void build_rejectsMaxQueueSizeAboveTwoMbCap() {
        var httpClient = new InMemoryReactiveHttpClient();

        assertThatThrownBy(() -> reporterBuilder(httpClient)
                .maxQueueSize(ApiUsageReporter.ABSOLUTE_MAX_QUEUE_SIZE + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("max queue size must not exceed " + ApiUsageReporter.ABSOLUTE_MAX_QUEUE_SIZE);
    }

    @Test
    void build_acceptsAbsoluteMaxQueueSize() {
        var httpClient = new InMemoryReactiveHttpClient();

        var reporter = reporterBuilder(httpClient)
                .maxQueueSize(ApiUsageReporter.ABSOLUTE_MAX_QUEUE_SIZE)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void report_triggersBackgroundFlushWhenQueueIsFull() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .maxQueueSize(2)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();
            assertThat(httpClient.requests()).isEmpty();

            scheduler.runUntilIdle();

            assertThat(httpClient.usageRequest(0).getEventsCount()).isEqualTo(2);
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void flushNowSendsWholeQueueInOneRequest() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .maxQueueSize(10)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();

            StepVerifier.create(reporter.flushNow())
                    .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(3))
                    .verifyComplete();

            assertThat(httpClient.usageRequests()).hasSize(1);
            assertThat(httpClient.usageRequest(0).getEventsCount()).isEqualTo(3);
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void scheduledFlushSendsQueuedUsageAtDefaultRandomBatchWindow() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = ApiUsageReporter.builder(httpClient)
                .host(URI.create("https://api.example.test"))
                .feddiGraphVariantKey("fddi_test_key")
                .scheduler(scheduler)
                .autoStart(true)
                .maxQueueSize(100)
                .randomSupplier(() -> 0.5)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(httpClient.requests()).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(29));
            assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
            assertThat(httpClient.usageRequests()).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(1));

            assertThat(httpClient.usageRequest(0).getEventsCount()).isEqualTo(1);
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void scheduledFlushResamplesBatchWindowAfterEachFlush() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var randomSupplier = new MutableDoubleSupplier(0.25);
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(20), Duration.ofSeconds(40))
                .randomSupplier(randomSupplier)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(24));
            assertThat(httpClient.usageRequests()).isEmpty();

            randomSupplier.set(0.75);
            scheduler.advanceBy(Duration.ofSeconds(1));
            assertThat(httpClient.usageRequest(0).getEventsCount()).isEqualTo(1);

            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(34));
            assertThat(httpClient.usageRequests()).hasSize(1);

            scheduler.advanceBy(Duration.ofSeconds(1));
            assertThat(httpClient.usageRequests()).hasSize(2);
        } finally {
            reporter.close();
        }
    }

    @Test
    void scheduledFlushReschedulesWhenQueueIsEmptyAndKnownOperationHashPrefetchIsInFlight() {
        var httpClient = new InMemoryReactiveHttpClient();
        var knownOperationHashesResponse = Sinks.<ReactiveHttpResponse>one();
        httpClient.enqueueKnownOperationHashesResponse(knownOperationHashesResponse.asMono());
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofMillis(200), Duration.ofMillis(400))
                .randomSupplier(() -> 0.0)
                .build();

        try {
            scheduler.advanceBy(Duration.ZERO);
            assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);

            scheduler.advanceBy(Duration.ofMillis(200));

            assertThat(httpClient.usageRequests()).isEmpty();
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);
        } finally {
            assertThat(knownOperationHashesResponse.tryEmitValue(InMemoryReactiveHttpClient.protobufResponse(
                    200,
                    KnownOperationHashesResponse.newBuilder().build()
            ))).isEqualTo(Sinks.EmitResult.OK);
            reporter.close();
        }
    }

    @Test
    void scheduledFlushRecoversFromUsageHttpErrorAndFlushesLaterUsage() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueUsageError(new IllegalStateException("usage down"));
        var errors = new CopyOnWriteArrayList<Throwable>();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .flushErrorHandler(errors::add)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(1));

            assertThat(errors)
                    .hasSize(1)
                    .first()
                    .satisfies(error -> assertThat(error)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("usage down"));
            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(httpClient.usageRequests()).hasSize(1);
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);

            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(1));

            assertThat(errors).hasSize(1);
            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(httpClient.usageRequests()).hasSize(2);
            assertThat(httpClient.usageRequest(1).getEventsCount()).isEqualTo(1);
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void scheduledFlushRetriesAfterKnownOperationHashPrefetchFailureWithInitiallyEmptyQueue() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueKnownOperationHashesStatus(503);
        var errors = new CopyOnWriteArrayList<Throwable>();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(10), Duration.ofSeconds(10))
                .randomSupplier(() -> 0.5)
                .flushErrorHandler(errors::add)
                .build();

        try {
            scheduler.advanceBy(Duration.ofSeconds(5));

            assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
            assertThat(errors).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(5));

            assertThat(errors).isEmpty();
            assertThat(httpClient.usageRequests()).isEmpty();
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);

            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(10));

            assertThat(errors)
                    .hasSize(1)
                    .first()
                    .satisfies(error -> assertThat(error)
                            .isInstanceOf(UsageReportClientException.class)
                            .hasMessage("Usage report request failed with HTTP status 503"));
            assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
            assertThat(httpClient.usageRequests()).isEmpty();
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);

            scheduler.advanceBy(Duration.ofSeconds(10));

            assertThat(errors).hasSize(1);
            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(httpClient.knownOperationHashesRequests()).hasSize(2);
            assertThat(httpClient.operationRequests()).hasSize(1);
            assertThat(httpClient.usageRequests()).hasSize(1);
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void backgroundFlushCompletesAndReschedulesWhenFlushErrorHandlerThrows() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueUsageError(new IllegalStateException("usage down"));
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .flushErrorHandler(error -> {
                    throw new IllegalStateException("handler failed", error);
                })
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(1));

            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(httpClient.usageRequests()).hasSize(1);
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);

            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(1));

            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(httpClient.usageRequests()).hasSize(2);
            assertThat(httpClient.usageRequest(1).getEventsCount()).isEqualTo(1);
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void canceledScheduledFlushDoesNotCreateDuplicateTimerWhenItRacesWithBackgroundFlush() {
        var httpClient = new InMemoryReactiveHttpClient();
        var usageResponse = Sinks.<ReactiveHttpResponse>one();
        httpClient.enqueueUsageResponse(usageResponse.asMono());
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .maxQueueSize(1)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            scheduler.runUntilIdle();

            assertThat(httpClient.usageRequests()).hasSize(1);
            assertThat(scheduler.activeScheduledTaskCount()).isZero();

            scheduler.runLastCancelledScheduledTask();

            assertThat(scheduler.activeScheduledTaskCount()).isZero();

            assertThat(usageResponse.tryEmitValue(InMemoryReactiveHttpClient.protobufResponse(
                    200,
                    UsageReportResponse.newBuilder().setAccepted(1).build()
            ))).isEqualTo(Sinks.EmitResult.OK);

            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(scheduler.activeScheduledTaskCount()).isEqualTo(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void closeAsyncCancelsScheduledFlushAndRejectsFutureReports() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.closeAsync())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(0).getEventsCount()).isEqualTo(1);
        assertThat(scheduler.activeScheduledTaskCount()).isZero();
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();
    }

    @Test
    void backgroundFlushTriggersAreCoalescedWhileFlushIsInFlight() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueResponse(Mono.never());
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .maxQueueSize(2)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            scheduler.advanceBy(Duration.ofSeconds(1));
            int requestsDuringInFlightFlush = httpClient.requests().size();
            assertThat(requestsDuringInFlightFlush).isGreaterThan(0);

            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();
            scheduler.runUntilIdle();

            assertThat(httpClient.requests()).hasSize(requestsDuringInFlightFlush);
        } finally {
            reporter.close();
        }
    }

    @Test
    void flush_recalculatesSamplingSamplesOutRequestsAndCapturesMultiplierOnQueuedEvents() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = reporterBuilder(httpClient)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .maxQueueSize(2000)
                .randomSupplier(randomSupplier)
                .samplingEnabled(true)
                .build();

        for (int i = 0; i < 150; i++) {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        }

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(150))
                .verifyComplete();

        assertThat(reporter.getSampleRate()).isEqualTo(0.1);
        assertThat(reporter.getMultiplier()).isEqualTo(10);

        randomSupplier.set(0.5);
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();

        randomSupplier.set(0.0);
        assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        var sampledRecord = httpClient.usageRequest(1).getEvents(0);
        assertThat(sampledRecord.hasMultiplier()).isTrue();
        assertThat(sampledRecord.getMultiplier()).isEqualTo(10);
    }

    @Test
    void flush_adjustsSamplingToOneHundredPercentBelowOneHundredRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.999);
        var reporter = samplingReporter(httpClient, randomSupplier);

        for (int i = 0; i < 99; i++) {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        }

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(99))
                .verifyComplete();

        assertThat(reporter.getSampleRate()).isEqualTo(1.0);
        assertThat(reporter.getMultiplier()).isEqualTo(1);

        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(1).getEvents(0).hasMultiplier()).isFalse();
    }

    @Test
    void flush_adjustsSamplingToTenPercentFromOneHundredRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = samplingReporter(httpClient, randomSupplier);

        queueUsage(reporter, 100);

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(100))
                .verifyComplete();

        assertThat(reporter.getSampleRate()).isEqualTo(0.1);
        assertThat(reporter.getMultiplier()).isEqualTo(10);

        randomSupplier.set(0.1);
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();

        randomSupplier.set(0.099);
        assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(1).getEvents(0).getMultiplier()).isEqualTo(10);
    }

    @Test
    void flush_adjustsSamplingToOnePercentFromOneThousandRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = samplingReporter(httpClient, randomSupplier);

        queueUsage(reporter, 1000);

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1000))
                .verifyComplete();

        assertThat(reporter.getSampleRate()).isEqualTo(0.01);
        assertThat(reporter.getMultiplier()).isEqualTo(100);

        randomSupplier.set(0.01);
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();

        randomSupplier.set(0.009);
        assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(1).getEvents(0).getMultiplier()).isEqualTo(100);
    }

    @Test
    void flush_adjustsSamplingToPointOnePercentFromTenThousandRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = samplingReporter(httpClient, randomSupplier);

        queueUsage(reporter, 10_000);

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(10_000))
                .verifyComplete();

        assertThat(reporter.getSampleRate()).isEqualTo(0.001);
        assertThat(reporter.getMultiplier()).isEqualTo(1000);

        randomSupplier.set(0.001);
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();

        randomSupplier.set(0.0009);
        assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(1).getEvents(0).getMultiplier()).isEqualTo(1000);
    }

    @Test
    void defaultSamplingDisabledQueuesEveryInvocationWithoutMultiplierAtHighRequestRate() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(1.0);
        var reporter = reporterBuilder(httpClient)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .maxQueueSize(20_000)
                .randomSupplier(randomSupplier)
                .build();

        queueUsage(reporter, 10_000);

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(10_000))
                .verifyComplete();

        assertThat(reporter.getSampleRate()).isEqualTo(1.0);
        assertThat(reporter.getMultiplier()).isEqualTo(1);
        assertThat(httpClient.usageRequest(0).getEventsList())
                .allSatisfy(event -> assertThat(event.hasMultiplier()).isFalse());

        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(1).getEvents(0).hasMultiplier()).isFalse();
    }

    @Test
    void flushNow_reportsAnalyzerFailuresToHandlerDropsInvalidUsageAndSkipsHttp() {
        var httpClient = new InMemoryReactiveHttpClient();
        var errors = new CopyOnWriteArrayList<Throwable>();
        var reporter = reporterBuilder(httpClient)
                .flushErrorHandler(errors::add)
                .build();

        assertThat(reporter.report(invocationWithoutOperationName("""
                query One {
                  user { id }
                }

                query Two {
                  user { name }
                }
                """))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isZero())
                .verifyComplete();

        assertThat(reporter.getDroppedCount()).isEqualTo(1);
        assertThat(errors)
                .hasSize(1)
                .first()
                .satisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("operationName is required when the document contains multiple operations"));
        assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
        assertThat(httpClient.operationRequests()).isEmpty();
        assertThat(httpClient.usageRequests()).isEmpty();
    }

    @Test
    void backgroundFlushReportsHttpErrorsToFlushErrorHandler() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueError(new IllegalStateException("network down"));
        var errors = new CopyOnWriteArrayList<Throwable>();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .maxQueueSize(1)
                .flushErrorHandler(errors::add)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(errors).isEmpty();

            scheduler.runUntilIdle();

            assertThat(httpClient.requests()).hasSize(3);
            assertThat(errors.getFirst())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("network down");
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void flushNowPropagatesHttpErrorsToCaller() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueStatus(200);
        httpClient.enqueueStatus(503);
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(UsageReportClientException.class)
                        .hasMessage("Usage report request failed with HTTP status 503"))
                .verify();

        assertThat(reporter.getPendingQueueSize()).isZero();
    }

    @Test
    void closeAsyncFlushesPendingUsageAndRejectsFutureReports() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.closeAsync())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(0).getEventsCount()).isEqualTo(1);
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();
    }

    @Test
    void knownOperationHashPrefetchSkipsAlreadyRegisteredOperationDefinition() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.addKnownOperationHash(operationHash("query GetUser { user { id } }"));
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(reporter.getKnownOperationCacheSize()).isEqualTo(1);
        assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
        assertThat(httpClient.operationRequests()).isEmpty();
        assertThat(httpClient.usageRequests()).hasSize(1);
    }

    @Test
    void knownOperationHashPrefetchFailureFailsFlushAndIsRetried() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueKnownOperationHashesStatus(503);
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(UsageReportClientException.class)
                        .hasMessage("Usage report request failed with HTTP status 503"))
                .verify();

        assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
        assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
        assertThat(httpClient.operationRequests()).isEmpty();
        assertThat(httpClient.usageRequests()).isEmpty();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(reporter.getPendingQueueSize()).isZero();
        assertThat(httpClient.knownOperationHashesRequests()).hasSize(2);
        assertThat(httpClient.operationRequests()).hasSize(1);
        assertThat(httpClient.usageRequests()).hasSize(1);
    }

    @Test
    void scheduledKnownOperationHashPrefetchFailureFailsFirstFlushAndRetriesLater() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueKnownOperationHashesStatus(503);
        var errors = new CopyOnWriteArrayList<Throwable>();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .flushErrorHandler(errors::add)
                .batchWindow(Duration.ofSeconds(10), Duration.ofSeconds(10))
                .randomSupplier(() -> 0.5)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

            scheduler.advanceBy(Duration.ofSeconds(5));

            assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
            assertThat(errors).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(5));

            assertThat(errors)
                    .hasSize(1)
                    .first()
                    .satisfies(error -> assertThat(error)
                            .isInstanceOf(UsageReportClientException.class)
                            .hasMessage("Usage report request failed with HTTP status 503"));
            assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
            assertThat(httpClient.operationRequests()).isEmpty();
            assertThat(httpClient.usageRequests()).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(10));

            assertThat(reporter.getPendingQueueSize()).isZero();
            assertThat(httpClient.knownOperationHashesRequests()).hasSize(2);
            assertThat(httpClient.operationRequests()).hasSize(1);
            assertThat(httpClient.usageRequests()).hasSize(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void knownOperationHashPrefetchIsScheduledWithJitterBeforeFirstFlush() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .autoStart(true)
                .batchWindow(Duration.ofSeconds(10), Duration.ofSeconds(10))
                .randomSupplier(() -> 0.5)
                .build();

        try {
            scheduler.advanceBy(Duration.ofMillis(4_999));
            assertThat(httpClient.knownOperationHashesRequests()).isEmpty();

            scheduler.advanceBy(Duration.ofMillis(1));
            assertThat(httpClient.knownOperationHashesRequests()).hasSize(1);
        } finally {
            reporter.close();
        }
    }

    @Test
    void build_rejectsInvalidBatchWindow() {
        var httpClient = new InMemoryReactiveHttpClient();

        assertThatThrownBy(() -> reporterBuilder(httpClient)
                .batchWindow(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("min batch window must be positive");

        assertThatThrownBy(() -> reporterBuilder(httpClient)
                .batchWindow(Duration.ofSeconds(2), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("min batch window must not exceed max batch window");
    }

    @Test
    void successfulOperationRegistrationCachesHashAndSkipsRepeatedRegistration() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        StepVerifier.create(reporter.flushNow())
                .expectNextCount(1)
                .verifyComplete();

        assertThat(reporter.getKnownOperationCacheSize()).isEqualTo(1);
        assertThat(httpClient.operationRequests()).hasSize(1);
        assertThat(httpClient.usageRequests()).hasSize(1);

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        StepVerifier.create(reporter.flushNow())
                .expectNextCount(1)
                .verifyComplete();

        assertThat(httpClient.operationRequests()).hasSize(1);
        assertThat(httpClient.usageRequests()).hasSize(2);
    }

    @Test
    void failedOperationRegistrationIsRetriedAndDoesNotBlockUsage() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueStatus(503);
        var errors = new CopyOnWriteArrayList<Throwable>();
        var reporter = reporterBuilder(httpClient)
                .flushErrorHandler(errors::add)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(errors).hasSize(1);
        assertThat(reporter.getKnownOperationCacheSize()).isZero();
        assertThat(reporter.getPendingOperationQueueSize()).isEqualTo(1);
        assertThat(httpClient.operationRequests()).hasSize(1);
        assertThat(httpClient.usageRequests()).hasSize(1);

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(reporter.getKnownOperationCacheSize()).isEqualTo(1);
        assertThat(reporter.getPendingOperationQueueSize()).isZero();
        assertThat(httpClient.operationRequests()).hasSize(2);
        assertThat(httpClient.usageRequests()).hasSize(1);
    }

    @Test
    void partialOperationRegistrationIsRetriedAndDoesNotCacheOperationHash() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueAccepted(0);
        var errors = new CopyOnWriteArrayList<Throwable>();
        var reporter = reporterBuilder(httpClient)
                .flushErrorHandler(errors::add)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(errors)
                .hasSize(1)
                .first()
                .satisfies(error -> assertThat(error)
                        .isInstanceOf(UsageReportClientException.class)
                        .hasMessage("Operation registration accepted 0 of 1 operation definitions"));
        assertThat(reporter.getKnownOperationCacheSize()).isZero();
        assertThat(reporter.getPendingOperationQueueSize()).isEqualTo(1);
        assertThat(httpClient.operationRequests()).hasSize(1);
        assertThat(httpClient.usageRequests()).hasSize(1);

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(reporter.getKnownOperationCacheSize()).isEqualTo(1);
        assertThat(reporter.getPendingOperationQueueSize()).isZero();
        assertThat(httpClient.operationRequests()).hasSize(2);
        assertThat(httpClient.usageRequests()).hasSize(1);
    }

    @Test
    void closeHandlesFlushErrorsWithFlushErrorHandler() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueError(new IllegalStateException("close failed"));
        var errors = new CopyOnWriteArrayList<Throwable>();
        var reporter = reporterBuilder(httpClient)
                .flushErrorHandler(errors::add)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        reporter.close();

        assertThat(errors)
                .hasSize(1)
                .first()
                .satisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("close failed"));
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();
    }

    private static ApiUsageReporter.Builder reporterBuilder(InMemoryReactiveHttpClient httpClient) {
        return reporterBuilder(httpClient, new ManualReporterScheduler());
    }

    private static ApiUsageReporter.Builder reporterBuilder(
            InMemoryReactiveHttpClient httpClient,
            ManualReporterScheduler scheduler
    ) {
        return ApiUsageReporter.builder(httpClient)
                .host(URI.create("https://api.example.test"))
                .feddiGraphVariantKey("fddi_test_key")
                .scheduler(scheduler)
                .autoStart(false)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .maxQueueSize(100)
                .randomSupplier(() -> 0.0);
    }

    private static ApiUsageReporter samplingReporter(
            InMemoryReactiveHttpClient httpClient,
            MutableDoubleSupplier randomSupplier
    ) {
        return reporterBuilder(httpClient)
                .batchWindow(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .maxQueueSize(20_000)
                .randomSupplier(randomSupplier)
                .samplingEnabled(true)
                .build();
    }

    private static void queueUsage(ApiUsageReporter reporter, int count) {
        for (int i = 0; i < count; i++) {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        }
    }

    private static String operationHash(String documentBody) {
        var usage = new ApiUsageDocumentAnalyzer().analyze(invocation(documentBody));
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(usage.canonicalDocument().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static ApiUsageInvocation invocation(String documentBody) {
        return invocation("GetUser", documentBody);
    }

    private static ApiUsageInvocation invocation(String operationName, String documentBody) {
        return invocation(operationName, documentBody, Map.of());
    }

    private static ApiUsageInvocation invocation(
            String operationName,
            String documentBody,
            Map<String, Object> variables
    ) {
        Document document = Parser.parse(documentBody);
        return ApiUsageInvocation.builder()
                .document(document)
                .operationName(operationName)
                .schema(schema())
                .variables(variables)
                .durationNanos(1_500_000)
                .clientName("web-app")
                .clientVersion("1.2.0")
                .timestamp(Instant.parse("2026-03-22T10:00:00Z"))
                .build();
    }

    private static ApiUsageInvocation invocationWithoutOperationName(String documentBody) {
        Document document = Parser.parse(documentBody);
        return ApiUsageInvocation.builder()
                .document(document)
                .schema(schema())
                .build();
    }

    private static GraphQLSchema schema() {
        var registry = new SchemaParser().parse("""
	                type Query {
	                  user(id: ID, filter: UserFilter): User
	                }

	                input UserFilter {
	                  name: String
	                  friend: UserFilter
	                }

	                type User {
                  id: ID!
                  name: String!
                  friend: User
                }
                """);
        return new SchemaGenerator().makeExecutableSchema(registry, RuntimeWiring.newRuntimeWiring().build());
    }

    private static final class MutableDoubleSupplier implements DoubleSupplier {

        private volatile double value;

        private MutableDoubleSupplier(double value) {
            this.value = value;
        }

        @Override
        public double getAsDouble() {
            return value;
        }

        private void set(double value) {
            this.value = value;
        }
    }

    private static final class ManualReporterScheduler implements ReporterScheduler {

        private final Queue<Runnable> immediateTasks = new ArrayDeque<>();
        private final List<ScheduledTask> scheduledTasks = new ArrayList<>();
        private long nowNanos;
        private boolean closed;

        @Override
        public void execute(Runnable task) {
            if (!closed) {
                immediateTasks.add(task);
            }
        }

        @Override
        public Cancellable schedule(Runnable task, Duration delay) {
            var scheduledTask = new ScheduledTask(
                    task,
                    nowNanos + toNanos(delay)
            );
            scheduledTasks.add(scheduledTask);
            return () -> scheduledTask.cancelled = true;
        }

        private void runUntilIdle() {
            int guard = 0;
            while (!immediateTasks.isEmpty() || nextDueTask(nowNanos) != null) {
                if (++guard > 10_000) {
                    throw new AssertionError("manual scheduler did not become idle");
                }
                runImmediateTasks();
                ScheduledTask scheduledTask = nextDueTask(nowNanos);
                if (scheduledTask != null) {
                    runScheduledTask(scheduledTask);
                }
            }
        }

        private void advanceBy(Duration duration) {
            long targetNanos = nowNanos + toNanos(duration);
            while (true) {
                runImmediateTasks();
                ScheduledTask scheduledTask = nextDueTask(targetNanos);
                if (scheduledTask == null) {
                    break;
                }
                nowNanos = scheduledTask.nextRunNanos;
                runScheduledTask(scheduledTask);
            }
            nowNanos = targetNanos;
            runUntilIdle();
        }

        private int activeScheduledTaskCount() {
            return (int) scheduledTasks.stream()
                    .filter(scheduledTask -> !scheduledTask.cancelled)
                    .count();
        }

        private void runLastCancelledScheduledTask() {
            ScheduledTask scheduledTask = scheduledTasks.stream()
                    .filter(task -> task.cancelled)
                    .max(Comparator.comparingLong(task -> task.nextRunNanos))
                    .orElseThrow();
            scheduledTask.task.run();
        }

        @Override
        public void close() {
            closed = true;
            immediateTasks.clear();
            scheduledTasks.clear();
        }

        private void runImmediateTasks() {
            Runnable task;
            while ((task = immediateTasks.poll()) != null) {
                task.run();
            }
        }

        private void runScheduledTask(ScheduledTask scheduledTask) {
            if (scheduledTask.cancelled) {
                return;
            }
            scheduledTask.cancelled = true;
            scheduledTask.task.run();
        }

        private ScheduledTask nextDueTask(long deadlineNanos) {
            return scheduledTasks.stream()
                    .filter(scheduledTask -> !scheduledTask.cancelled)
                    .filter(scheduledTask -> scheduledTask.nextRunNanos <= deadlineNanos)
                    .min(Comparator.comparingLong(scheduledTask -> scheduledTask.nextRunNanos))
                    .orElse(null);
        }

        private static long toNanos(Duration duration) {
            if (duration.isNegative()) {
                throw new IllegalArgumentException("duration must not be negative");
            }
            return duration.toNanos();
        }

        private static final class ScheduledTask {

            private final Runnable task;
            private long nextRunNanos;
            private boolean cancelled;

            private ScheduledTask(Runnable task, long nextRunNanos) {
                this.task = task;
                this.nextRunNanos = nextRunNanos;
            }
        }
    }

    private static final class InMemoryReactiveHttpClient implements ReactiveHttpClient {

        private final List<ReactiveHttpRequest> requests = new CopyOnWriteArrayList<>();
        private final Queue<Mono<ReactiveHttpResponse>> responses = new ConcurrentLinkedQueue<>();
        private final Queue<Mono<ReactiveHttpResponse>> operationResponses = new ConcurrentLinkedQueue<>();
        private final Queue<Mono<ReactiveHttpResponse>> usageResponses = new ConcurrentLinkedQueue<>();
        private final Queue<Mono<ReactiveHttpResponse>> knownOperationHashesResponses = new ConcurrentLinkedQueue<>();
        private final List<String> knownOperationHashes = new CopyOnWriteArrayList<>();

        @Override
        public Mono<ReactiveHttpResponse> exchange(ReactiveHttpRequest request) {
            requests.add(request);
            if (request.uri().getPath().endsWith("/known-operation-hashes")) {
                Mono<ReactiveHttpResponse> queuedResponse = knownOperationHashesResponses.poll();
                if (queuedResponse != null) {
                    return queuedResponse;
                }
                return Mono.just(protobufResponse(200, KnownOperationHashesResponse.newBuilder()
                        .addAllOperationHashes(knownOperationHashes)
                        .build()));
            }

            if (request.uri().getPath().endsWith("/operations")) {
                return responseForEndpoint(operationResponses)
                        .switchIfEmpty(request.body()
                                .map(body -> parseOperationRequest(body).getOperationsCount())
                                .map(accepted -> protobufResponse(200, UsageReportResponse.newBuilder()
                                        .setAccepted(accepted)
                                        .build())));
            }

            return responseForEndpoint(usageResponses)
                    .switchIfEmpty(request.body()
                            .map(body -> parseUsageRequest(body).getEventsCount())
                            .map(accepted -> protobufResponse(200, UsageReportResponse.newBuilder()
                                    .setAccepted(accepted)
                                    .build())));
        }

        private void addKnownOperationHash(String operationHash) {
            knownOperationHashes.add(operationHash);
        }

        private void enqueueKnownOperationHashesStatus(int statusCode) {
            knownOperationHashesResponses.add(Mono.just(new ReactiveHttpResponse(statusCode, Map.of(), Mono.just(new byte[0]))));
        }

        private void enqueueKnownOperationHashesResponse(Mono<ReactiveHttpResponse> response) {
            knownOperationHashesResponses.add(response);
        }

        private void enqueueStatus(int statusCode) {
            responses.add(Mono.just(new ReactiveHttpResponse(statusCode, Map.of(), Mono.just(new byte[0]))));
        }

        private void enqueueAccepted(int accepted) {
            responses.add(Mono.just(protobufResponse(200, UsageReportResponse.newBuilder()
                    .setAccepted(accepted)
                    .build())));
        }

        private void enqueueError(Throwable error) {
            responses.add(Mono.error(error));
        }

        private void enqueueResponse(Mono<ReactiveHttpResponse> response) {
            responses.add(response);
        }

        private void enqueueUsageError(Throwable error) {
            usageResponses.add(Mono.error(error));
        }

        private void enqueueUsageResponse(Mono<ReactiveHttpResponse> response) {
            usageResponses.add(response);
        }

        private Mono<ReactiveHttpResponse> responseForEndpoint(Queue<Mono<ReactiveHttpResponse>> endpointResponses) {
            Mono<ReactiveHttpResponse> queuedResponse = endpointResponses.poll();
            if (queuedResponse != null) {
                return queuedResponse;
            }
            queuedResponse = responses.poll();
            if (queuedResponse != null) {
                return queuedResponse;
            }
            return Mono.empty();
        }

        private List<ReactiveHttpRequest> requests() {
            return requests;
        }

        private List<ReactiveHttpRequest> knownOperationHashesRequests() {
            return requests.stream()
                    .filter(request -> request.uri().getPath().endsWith("/known-operation-hashes"))
                    .toList();
        }

        private ReactiveHttpRequest requestEndingWith(String pathSuffix) {
            return requests.stream()
                    .filter(request -> request.uri().getPath().endsWith(pathSuffix))
                    .findFirst()
                    .orElseThrow();
        }

        private RegisterOperationsRequest operationRequest(int index) {
            return operationRequests().get(index);
        }

        private List<RegisterOperationsRequest> operationRequests() {
            return requests.stream()
                    .filter(request -> request.uri().getPath().endsWith("/operations"))
                    .map(InMemoryReactiveHttpClient::parseOperationRequest)
                    .toList();
        }

        private IngestUsageRequest usageRequest(int index) {
            return usageRequests().get(index);
        }

        private List<IngestUsageRequest> usageRequests() {
            return requests.stream()
                    .filter(request -> request.uri().getPath().endsWith("/usage"))
                    .map(InMemoryReactiveHttpClient::parseUsageRequest)
                    .toList();
        }

        private static ReactiveHttpResponse protobufResponse(int statusCode, UsageReportResponse response) {
            return new ReactiveHttpResponse(statusCode, Map.of(), Mono.just(response.toByteArray()));
        }

        private static ReactiveHttpResponse protobufResponse(int statusCode, KnownOperationHashesResponse response) {
            return new ReactiveHttpResponse(statusCode, Map.of(), Mono.just(response.toByteArray()));
        }

        private static RegisterOperationsRequest parseOperationRequest(ReactiveHttpRequest request) {
            byte[] body = request.body().block(Duration.ofSeconds(1));
            return parseOperationRequest(body);
        }

        private static RegisterOperationsRequest parseOperationRequest(byte[] body) {
            try {
                return RegisterOperationsRequest.parseFrom(gunzip(body));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private static IngestUsageRequest parseUsageRequest(ReactiveHttpRequest request) {
            byte[] body = request.body().block(Duration.ofSeconds(1));
            return parseUsageRequest(body);
        }

        private static IngestUsageRequest parseUsageRequest(byte[] body) {
            try {
                return IngestUsageRequest.parseFrom(gunzip(body));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private static byte[] gunzip(byte[] body) {
            try (var input = new GZIPInputStream(new ByteArrayInputStream(body));
                 var output = new ByteArrayOutputStream()) {
                input.transferTo(output);
                return output.toByteArray();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }
}
