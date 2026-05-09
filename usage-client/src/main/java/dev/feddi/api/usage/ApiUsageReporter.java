package dev.feddi.api.usage;

import com.google.protobuf.Timestamp;
import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.v1.InputUsageCoordinate;
import dev.feddi.api.usage.v1.UsageRecord;
import dev.feddi.api.usage.v1.UsageReportRequest;
import dev.feddi.api.usage.v1.UsageReportResponse;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;

/**
 * High-level reporter for sending GraphQL API usage events to the feddi Platform.
 *
 * <p>Create one reporter for a Feddi graph variant key and keep it for the lifetime of the API
 * process. Each completed GraphQL request should be passed to {@link #report(ApiUsageInvocation)}
 * with the parsed GraphQL Java {@link graphql.language.Document}, operation name, executable
 * {@link graphql.schema.GraphQLSchema}, timing, and error metadata.
 *
     * <p>The reporter derives the canonical operation document, field coordinates, field argument
     * coordinates, and input object field coordinates locally, batches records in memory, and sends
     * protobuf batches to the platform. The platform calculates the operation hash from the canonical
     * document on ingest.
 *
 * <p>HTTP transport is deliberately pluggable. Applications provide a {@link ReactiveHttpClient}
 * implementation, usually backed by the HTTP client they already use. The default host is
 * {@code https://feddi.dev}; the endpoint path is fixed to {@code /api/usage-proto}.
 *
 * <p>Instances support concurrent {@code report(...)} calls. Call {@link #closeAsync()} or
 * {@link #close()} during shutdown to flush queued usage before the process exits.
 */
public final class ApiUsageReporter implements AutoCloseable {

    /**
     * Default maximum number of usage records sent in one protobuf request.
     */
    public static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Default maximum number of pending invocations held in memory before new events are dropped.
     */
    public static final int DEFAULT_MAX_QUEUE_SIZE = 10_000;

    /**
     * Default interval for background flushing.
     */
    public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(10);

    private final UsageReportClient client;
    private final ApiUsageDocumentAnalyzer analyzer;
    private final ConcurrentLinkedQueue<PendingUsage> pendingQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final Object lifecycleLock = new Object();
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final Duration flushInterval;
    private final ReporterScheduler scheduler;
    private final ReporterScheduler.Cancellable scheduledFlush;
    private final AtomicLong requestCounter = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Consumer<Throwable> flushErrorHandler;
    private final DoubleSupplier randomSupplier;
    private volatile double sampleRate = 1.0;
    private volatile int multiplier = 1;

    private ApiUsageReporter(Builder builder) {
        this.client = new UsageReportClient(
                builder.httpClient,
                builder.host,
                requireNonBlank(builder.feddiGraphVariantKey, "feddiGraphVariantKey")
        );
        this.analyzer = new ApiUsageDocumentAnalyzer();
        this.maxBatchSize = builder.maxBatchSize > 0 ? builder.maxBatchSize : DEFAULT_BATCH_SIZE;
        this.maxQueueSize = builder.maxQueueSize > 0 ? builder.maxQueueSize : DEFAULT_MAX_QUEUE_SIZE;
        this.flushInterval = builder.flushInterval != null && !builder.flushInterval.isNegative() && !builder.flushInterval.isZero()
                ? builder.flushInterval
                : DEFAULT_FLUSH_INTERVAL;
        this.flushErrorHandler = builder.flushErrorHandler != null ? builder.flushErrorHandler : ignored -> {};
        this.randomSupplier = builder.randomSupplier != null
                ? builder.randomSupplier
                : () -> ThreadLocalRandom.current().nextDouble();
        this.scheduler = builder.scheduler != null ? builder.scheduler : new ScheduledExecutorReporterScheduler();

        if (builder.autoStart) {
            this.scheduledFlush = scheduler.scheduleAtFixedRate(
                    this::processAndFlushSafely,
                    this.flushInterval,
                    this.flushInterval
            );
        } else {
            this.scheduledFlush = () -> {};
        }
    }

    /**
     * Starts building an {@link ApiUsageReporter}.
     *
     * @param httpClient reactive HTTP transport used for protobuf POST requests
     * @return a reporter builder
     */
    public static Builder builder(ReactiveHttpClient httpClient) {
        return new Builder(httpClient);
    }

    /**
     * Queues a completed GraphQL invocation for reporting.
     *
     * <p>This method does not perform network I/O. It records the invocation in an in-memory queue
     * and returns immediately. The invocation may be sampled out or dropped if the reporter is closed
     * or the queue is full.
     *
     * @param invocation completed API invocation to report
     * @return {@code true} when the invocation was queued, otherwise {@code false}
     */
    public boolean report(ApiUsageInvocation invocation) {
        Objects.requireNonNull(invocation, "invocation");
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return false;
            }

            requestCounter.incrementAndGet();
            if (randomSupplier.getAsDouble() >= sampleRate) {
                return false;
            }

            if (pendingCount.get() >= maxQueueSize) {
                droppedCount.incrementAndGet();
                return false;
            }

            pendingCount.incrementAndGet();
            pendingQueue.add(new PendingUsage(invocation, multiplier));
            if (pendingCount.get() >= maxBatchSize) {
                scheduler.execute(this::processAndFlushSafely);
            }
        }
        return true;
    }

    /**
     * Flushes one batch immediately.
     *
     * <p>The returned {@link Mono} is cold; flushing starts when it is subscribed to. If no records
     * are queued, the returned response has {@code accepted == 0} and no HTTP request is made.
     *
     * @return response from the platform, or an accepted-zero response when there is nothing to send
     */
    public Mono<UsageReportResponse> flushNow() {
        return Mono.defer(this::processAndFlush);
    }

    /**
     * Stops background flushing, rejects future reports, and flushes the remaining queue.
     *
     * @return response from the final flush
     */
    public Mono<UsageReportResponse> closeAsync() {
        synchronized (lifecycleLock) {
            if (closed.compareAndSet(false, true)) {
                scheduledFlush.cancel();
                scheduler.close();
            }
        }
        return flushNow();
    }

    /**
     * Synchronously closes the reporter and waits up to ten seconds for the final flush.
     *
     * <p>Flush errors are passed to the configured flush error handler and are not rethrown.
     */
    @Override
    public void close() {
        closeAsync()
                .onErrorResume(error -> {
                    flushErrorHandler.accept(error);
                    return Mono.empty();
                })
                .block(Duration.ofSeconds(10));
    }

    private void processAndFlushSafely() {
        processAndFlush()
                .subscribe(
                        ignored -> {},
                        flushErrorHandler
                );
    }

    private Mono<UsageReportResponse> processAndFlush() {
        recalculateSampling();

        var request = drainBatch();
        if (request.getRecordsCount() == 0) {
            return Mono.just(UsageReportResponse.newBuilder().setAccepted(0).build());
        }

        return client.report(request);
    }

    private void recalculateSampling() {
        long count = requestCounter.getAndSet(0);
        double rps = count / (flushInterval.toMillis() / 1000.0);
        if (rps < 100) {
            sampleRate = 1.0;
            multiplier = 1;
        } else if (rps < 1000) {
            sampleRate = 0.1;
            multiplier = 10;
        } else if (rps < 10000) {
            sampleRate = 0.01;
            multiplier = 100;
        } else {
            sampleRate = 0.001;
            multiplier = 1000;
        }
    }

    private UsageReportRequest drainBatch() {
        var records = new ArrayList<UsageRecord>(maxBatchSize);
        PendingUsage pending;
        while (records.size() < maxBatchSize && (pending = pendingQueue.poll()) != null) {
            pendingCount.decrementAndGet();
            try {
                records.add(toProto(pending));
            } catch (RuntimeException e) {
                droppedCount.incrementAndGet();
                flushErrorHandler.accept(e);
            }
        }

        return UsageReportRequest.newBuilder()
                .addAllRecords(records)
                .build();
    }

    private UsageRecord toProto(PendingUsage pending) {
        var invocation = pending.invocation();
        var usage = analyzer.analyze(invocation);

        var builder = UsageRecord.newBuilder()
                .setOperationName(nullToEmpty(usage.operationName()))
                .setOperationType(usage.operationType())
                .setCanonicalDocument(usage.canonicalDocument())
                .addAllFieldCoordinates(usage.fieldCoordinates())
                .addAllInputUsageCoordinates(usage.inputUsageCoordinates().stream()
                        .map(inputUsage -> InputUsageCoordinate.newBuilder()
                                .setCoordinate(inputUsage.coordinate())
                                .setKind(inputUsage.kind())
                                .build())
                        .toList())
                .setDurationNanos(invocation.durationNanos())
                .setHttpError(invocation.httpError())
                .setGraphqlError(invocation.graphqlError())
                .setClientName(invocation.clientName())
                .setClientVersion(invocation.clientVersion())
                .setTimestamp(toTimestamp(invocation.timestamp()));

        if (pending.multiplier() > 1) {
            builder.setMultiplier(pending.multiplier());
        }

        return builder.build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static String nullToEmpty(@Nullable String value) {
        return value == null ? "" : value;
    }

    private static String requireNonBlank(@Nullable String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    double getSampleRate() {
        return sampleRate;
    }

    int getMultiplier() {
        return multiplier;
    }

    int getPendingQueueSize() {
        return pendingCount.get();
    }

    long getDroppedCount() {
        return droppedCount.get();
    }

    private record PendingUsage(ApiUsageInvocation invocation, int multiplier) {
    }

    /**
     * Builder for {@link ApiUsageReporter}.
     *
     * <p>The only required values are the {@link ReactiveHttpClient} passed to
     * {@link ApiUsageReporter#builder(ReactiveHttpClient)} and the Feddi graph variant key supplied
     * via {@link #feddiGraphVariantKey(String)}. All other settings have production defaults.
     */
    public static final class Builder {

        private final ReactiveHttpClient httpClient;
        private URI host = UsageReportClient.DEFAULT_PLATFORM_HOST;
        private @Nullable String feddiGraphVariantKey;
        private int maxBatchSize = DEFAULT_BATCH_SIZE;
        private int maxQueueSize = DEFAULT_MAX_QUEUE_SIZE;
        private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
        private boolean autoStart = true;
        private @Nullable Consumer<Throwable> flushErrorHandler;
        private @Nullable DoubleSupplier randomSupplier;
        private @Nullable ReporterScheduler scheduler;

        private Builder(ReactiveHttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        }

        /**
         * Sets the Feddi graph variant key used as the bearer token for usage ingestion.
         *
         * @param feddiGraphVariantKey token created by the feddi Platform for the target graph variant
         * @return this builder
         */
        public Builder feddiGraphVariantKey(String feddiGraphVariantKey) {
            this.feddiGraphVariantKey = feddiGraphVariantKey;
            return this;
        }

        /**
         * Overrides the platform host.
         *
         * <p>The URI must be absolute and must not include a path other than an optional trailing
         * slash, query, or fragment. The endpoint path remains fixed to {@code /api/usage-proto}.
         *
         * @param host platform host, for example {@code https://feddi.dev}
         * @return this builder
         */
        public Builder host(URI host) {
            this.host = Objects.requireNonNull(host, "host");
            return this;
        }

        /**
         * Overrides the platform host.
         *
         * @param host absolute platform host, for example {@code https://feddi.dev}
         * @return this builder
         * @see #host(URI)
         */
        public Builder host(String host) {
            return host(URI.create(requireNonBlank(host, "host")));
        }

        /**
         * Sets the maximum number of usage records sent in one protobuf request.
         *
         * <p>Values less than or equal to zero use {@link ApiUsageReporter#DEFAULT_BATCH_SIZE}.
         *
         * @param maxBatchSize maximum batch size
         * @return this builder
         */
        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        /**
         * Sets the maximum number of pending invocations held in memory.
         *
         * <p>When the queue is full, new usage events are dropped and
         * {@link ApiUsageReporter#report(ApiUsageInvocation)} returns {@code false}. Values less
         * than or equal to zero use {@link ApiUsageReporter#DEFAULT_MAX_QUEUE_SIZE}.
         *
         * @param maxQueueSize maximum pending queue size
         * @return this builder
         */
        public Builder maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        /**
         * Sets the background flush interval.
         *
         * <p>Zero or negative durations use {@link ApiUsageReporter#DEFAULT_FLUSH_INTERVAL}.
         *
         * @param flushInterval interval between automatic flushes
         * @return this builder
         */
        public Builder flushInterval(Duration flushInterval) {
            this.flushInterval = Objects.requireNonNull(flushInterval, "flushInterval");
            return this;
        }

        /**
         * Enables or disables scheduled background flushing.
         *
         * <p>Disabling auto-start is mainly useful in tests or in hosts that want to call
         * {@link ApiUsageReporter#flushNow()} explicitly.
         *
         * @param autoStart whether to start scheduled flushing when the reporter is built
         * @return this builder
         */
        public Builder autoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        /**
         * Handles errors from background flushes and per-record analysis failures.
         *
         * <p>If no handler is configured, these errors are ignored.
         *
         * @param flushErrorHandler error handler
         * @return this builder
         */
        public Builder flushErrorHandler(Consumer<Throwable> flushErrorHandler) {
            this.flushErrorHandler = Objects.requireNonNull(flushErrorHandler, "flushErrorHandler");
            return this;
        }

        Builder randomSupplier(DoubleSupplier randomSupplier) {
            this.randomSupplier = Objects.requireNonNull(randomSupplier, "randomSupplier");
            return this;
        }

        Builder scheduler(ReporterScheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            return this;
        }

        /**
         * Builds the reporter.
         *
         * @return configured reporter
         * @throws IllegalArgumentException if the Feddi graph variant key is missing or blank
         */
        public ApiUsageReporter build() {
            return new ApiUsageReporter(this);
        }
    }
}
