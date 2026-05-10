package dev.feddi.api.usage;

import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.http.ReactiveHttpResponse;
import dev.feddi.api.usage.v1.UsageReportRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import graphql.language.Document;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.DoubleSupplier;

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
    void flushNow_isColdAndSendsQueuedUsageOnlyWhenSubscribed() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .maxBatchSize(100)
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
        assertThat(httpClient.requests()).hasSize(1);
    }

    @Test
    void flushNow_returnsAcceptedZeroAndDoesNotCallHttpWhenQueueIsEmpty() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient).build();

        StepVerifier.create(reporter.flushNow())
                .assertNext(response -> assertThat(response.getAccepted()).isZero())
                .verifyComplete();

        assertThat(httpClient.requests()).isEmpty();
    }

    @Test
    void flushNow_sendsQueuedRecordsWithMetadataHeadersAndDefaultEndpoint() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = ApiUsageReporter.builder(httpClient)
                .feddiGraphVariantKey("fddi_test_key")
                .scheduler(new ManualReporterScheduler())
                .autoStart(false)
                .flushInterval(Duration.ofSeconds(1))
                .maxBatchSize(100)
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

        var httpRequest = httpClient.requests().getFirst();
        assertThat(httpRequest.method()).isEqualTo("POST");
        assertThat(httpRequest.uri()).isEqualTo(URI.create("https://feddi.dev/api/usage-proto"));
        assertThat(httpRequest.headers()).containsEntry("Authorization", List.of("Bearer fddi_test_key"));
        assertThat(httpRequest.headers()).containsEntry("Content-Type", List.of("application/x-protobuf"));
        assertThat(httpRequest.headers()).containsEntry("Accept", List.of("application/x-protobuf"));

        var record = httpClient.usageRequest(0).getRecords(0);
        assertThat(record.getOperationName()).isEqualTo("GetUser");
        assertThat(record.getOperationType()).isEqualTo("QUERY");
        assertThat(record.getCanonicalDocument()).isNotBlank();
        assertThat(record.getFieldCoordinatesList()).containsExactly(
                "Query.user",
                "User.friend",
                "User.id",
                "User.name"
        );
        assertThat(record.getDurationNanos()).isEqualTo(1_500_000);
        assertThat(record.getClientName()).isEqualTo("web-app");
        assertThat(record.getClientVersion()).isEqualTo("1.2.0");
        assertThat(record.getTimestamp().getSeconds())
                .isEqualTo(Instant.parse("2026-03-22T10:00:00Z").getEpochSecond());
    }

    @Test
    void flushNow_sendsInputUsageCoordinates() {
        var httpClient = new InMemoryReactiveHttpClient();
        var reporter = reporterBuilder(httpClient)
                .maxBatchSize(100)
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

        var record = httpClient.usageRequest(0).getRecords(0);
        assertThat(record.getInputUsageCoordinatesList())
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
                .maxBatchSize(100)
                .maxQueueSize(1)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();

        assertThat(reporter.getDroppedCount()).isEqualTo(1);
        assertThat(reporter.getPendingQueueSize()).isEqualTo(1);
        assertThat(httpClient.requests()).isEmpty();
    }

    @Test
    void report_triggersBackgroundFlushWhenBatchSizeIsReached() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .maxBatchSize(2)
                .maxQueueSize(10)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();
            assertThat(httpClient.requests()).isEmpty();

            scheduler.runUntilIdle();

            assertThat(httpClient.usageRequest(0).getRecordsCount()).isEqualTo(2);
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void flushesNeverSendMoreThanMaxBatchSizeRecords() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .maxBatchSize(2)
                .maxQueueSize(10)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isTrue();
            assertThat(reporter.report(invocation("query GetUser { user { friend { id } } }"))).isTrue();

            scheduler.runUntilIdle();
            assertThat(httpClient.usageRequest(0).getRecordsCount()).isEqualTo(2);

            StepVerifier.create(reporter.flushNow())
                    .expectNextCount(1)
                    .verifyComplete();

            var recordCounts = httpClient.usageRequests().stream()
                    .map(UsageReportRequest::getRecordsCount)
                    .toList();
            assertThat(recordCounts).allMatch(recordCount -> recordCount <= 2);
            assertThat(recordCounts.stream().mapToInt(Integer::intValue).sum()).isEqualTo(3);
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void scheduledFlushSendsQueuedUsageAtFlushInterval() {
        var httpClient = new InMemoryReactiveHttpClient();
        var scheduler = new ManualReporterScheduler();
        var reporter = ApiUsageReporter.builder(httpClient)
                .host(URI.create("https://api.example.test"))
                .feddiGraphVariantKey("fddi_test_key")
                .scheduler(scheduler)
                .autoStart(true)
                .flushInterval(Duration.ofSeconds(10))
                .maxBatchSize(100)
                .maxQueueSize(100)
                .randomSupplier(() -> 0.0)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(httpClient.requests()).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(9));
            assertThat(httpClient.requests()).isEmpty();

            scheduler.advanceBy(Duration.ofSeconds(1));

            assertThat(httpClient.usageRequest(0).getRecordsCount()).isEqualTo(1);
            assertThat(reporter.getPendingQueueSize()).isZero();
        } finally {
            reporter.close();
        }
    }

    @Test
    void flush_recalculatesSamplingSamplesOutRequestsAndCapturesMultiplierOnQueuedEvents() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = reporterBuilder(httpClient)
                .flushInterval(Duration.ofSeconds(1))
                .maxBatchSize(1000)
                .maxQueueSize(2000)
                .randomSupplier(randomSupplier)
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

        var sampledRecord = httpClient.usageRequest(1).getRecords(0);
        assertThat(sampledRecord.hasMultiplier()).isTrue();
        assertThat(sampledRecord.getMultiplier()).isEqualTo(10);
    }

    @Test
    void flush_adjustsSamplingToOneHundredPercentBelowOneHundredRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.999);
        var reporter = samplingReporter(httpClient, randomSupplier, 100);

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

        assertThat(httpClient.usageRequest(1).getRecords(0).hasMultiplier()).isFalse();
    }

    @Test
    void flush_adjustsSamplingToTenPercentFromOneHundredRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = samplingReporter(httpClient, randomSupplier, 200);

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

        assertThat(httpClient.usageRequest(1).getRecords(0).getMultiplier()).isEqualTo(10);
    }

    @Test
    void flush_adjustsSamplingToOnePercentFromOneThousandRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = samplingReporter(httpClient, randomSupplier, 1000);

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

        assertThat(httpClient.usageRequest(1).getRecords(0).getMultiplier()).isEqualTo(100);
    }

    @Test
    void flush_adjustsSamplingToPointOnePercentFromTenThousandRequestsPerSecond() {
        var httpClient = new InMemoryReactiveHttpClient();
        var randomSupplier = new MutableDoubleSupplier(0.0);
        var reporter = samplingReporter(httpClient, randomSupplier, 10_000);

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

        assertThat(httpClient.usageRequest(1).getRecords(0).getMultiplier()).isEqualTo(1000);
    }

    @Test
    void flushNow_reportsAnalyzerFailuresToHandlerDropsInvalidUsageAndSkipsHttp() {
        var httpClient = new InMemoryReactiveHttpClient();
        var errors = new CopyOnWriteArrayList<Throwable>();
        var reporter = reporterBuilder(httpClient)
                .flushErrorHandler(errors::add)
                .maxBatchSize(100)
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
        assertThat(httpClient.requests()).isEmpty();
    }

    @Test
    void backgroundFlushReportsHttpErrorsToFlushErrorHandler() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueError(new IllegalStateException("network down"));
        var errors = new CopyOnWriteArrayList<Throwable>();
        var scheduler = new ManualReporterScheduler();
        var reporter = reporterBuilder(httpClient, scheduler)
                .flushErrorHandler(errors::add)
                .maxBatchSize(1)
                .build();

        try {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
            assertThat(errors).isEmpty();

            scheduler.runUntilIdle();

            assertThat(httpClient.requests()).hasSize(1);
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
        httpClient.enqueueStatus(503);
        var reporter = reporterBuilder(httpClient)
                .maxBatchSize(100)
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
                .maxBatchSize(100)
                .build();

        assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();

        StepVerifier.create(reporter.closeAsync())
                .assertNext(response -> assertThat(response.getAccepted()).isEqualTo(1))
                .verifyComplete();

        assertThat(httpClient.usageRequest(0).getRecordsCount()).isEqualTo(1);
        assertThat(reporter.report(invocation("query GetUser { user { name } }"))).isFalse();
    }

    @Test
    void closeHandlesFlushErrorsWithFlushErrorHandler() {
        var httpClient = new InMemoryReactiveHttpClient();
        httpClient.enqueueError(new IllegalStateException("close failed"));
        var errors = new CopyOnWriteArrayList<Throwable>();
        var reporter = reporterBuilder(httpClient)
                .flushErrorHandler(errors::add)
                .maxBatchSize(100)
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
                .flushInterval(Duration.ofSeconds(1))
                .maxQueueSize(100)
                .randomSupplier(() -> 0.0);
    }

    private static ApiUsageReporter samplingReporter(
            InMemoryReactiveHttpClient httpClient,
            MutableDoubleSupplier randomSupplier,
            int maxBatchSize
    ) {
        return reporterBuilder(httpClient)
                .flushInterval(Duration.ofSeconds(1))
                .maxBatchSize(maxBatchSize)
                .maxQueueSize(20_000)
                .randomSupplier(randomSupplier)
                .build();
    }

    private static void queueUsage(ApiUsageReporter reporter, int count) {
        for (int i = 0; i < count; i++) {
            assertThat(reporter.report(invocation("query GetUser { user { id } }"))).isTrue();
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
        public Cancellable scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period) {
            var scheduledTask = new ScheduledTask(
                    task,
                    nowNanos + toNanos(initialDelay),
                    toNanos(period)
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
            scheduledTask.nextRunNanos += scheduledTask.periodNanos;
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
            private final long periodNanos;
            private long nextRunNanos;
            private boolean cancelled;

            private ScheduledTask(Runnable task, long nextRunNanos, long periodNanos) {
                this.task = task;
                this.nextRunNanos = nextRunNanos;
                this.periodNanos = periodNanos;
            }
        }
    }

    private static final class InMemoryReactiveHttpClient implements ReactiveHttpClient {

        private final List<ReactiveHttpRequest> requests = new CopyOnWriteArrayList<>();
        private final Queue<Mono<ReactiveHttpResponse>> responses = new ConcurrentLinkedQueue<>();

        @Override
        public Mono<ReactiveHttpResponse> exchange(ReactiveHttpRequest request) {
            requests.add(request);
            Mono<ReactiveHttpResponse> queuedResponse = responses.poll();
            if (queuedResponse != null) {
                return queuedResponse;
            }

            return request.body()
                    .map(InMemoryReactiveHttpClient::parseUsageRequest)
                    .map(usageRequest -> protobufResponse(200, UsageReportResponse.newBuilder()
                            .setAccepted(usageRequest.getRecordsCount())
                            .build()));
        }

        private void enqueueStatus(int statusCode) {
            responses.add(Mono.just(new ReactiveHttpResponse(statusCode, Map.of(), Mono.just(new byte[0]))));
        }

        private void enqueueError(Throwable error) {
            responses.add(Mono.error(error));
        }

        private List<ReactiveHttpRequest> requests() {
            return requests;
        }

        private UsageReportRequest usageRequest(int index) {
            return parseUsageRequest(requests.get(index));
        }

        private List<UsageReportRequest> usageRequests() {
            return requests.stream()
                    .map(InMemoryReactiveHttpClient::parseUsageRequest)
                    .toList();
        }

        private static ReactiveHttpResponse protobufResponse(int statusCode, UsageReportResponse response) {
            return new ReactiveHttpResponse(statusCode, Map.of(), Mono.just(response.toByteArray()));
        }

        private static UsageReportRequest parseUsageRequest(ReactiveHttpRequest request) {
            byte[] body = request.body().block(Duration.ofSeconds(1));
            return parseUsageRequest(body);
        }

        private static UsageReportRequest parseUsageRequest(byte[] body) {
            try {
                return UsageReportRequest.parseFrom(body);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }
}
