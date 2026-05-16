package dev.feddi.api.usage;

import com.google.protobuf.Timestamp;
import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.v1.IngestUsageRequest;
import dev.feddi.api.usage.v1.InputUsageCoordinate;
import dev.feddi.api.usage.v1.OperationDefinition;
import dev.feddi.api.usage.v1.RegisterOperationsRequest;
import dev.feddi.api.usage.v1.UsageEvent;
import dev.feddi.api.usage.v1.UsageReportResponse;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * coordinates, used directives, directive argument coordinates, and input object field coordinates
 * locally, batches records in memory, and sends protobuf batches to the platform. The platform
 * stores operation definitions separately from usage events. The reporter calculates the operation
 * hash from the canonical document, registers unknown operations in gzipped protobuf batches, and
 * sends usage batches containing only operation hashes and request-specific metadata.
 *
 * <p>The batch window controls how often the background scheduler attempts to flush queued usage.
 * The queue size controls how many usage records can wait for that flush. Each automatic flush
 * drains the whole queue as it exists when draining starts, so the largest automatic usage request
 * is bounded by {@link #DEFAULT_MAX_QUEUE_SIZE} by default and by
 * {@link #ABSOLUTE_MAX_QUEUE_SIZE} for any custom configuration. If the queue reaches the configured
 * limit before the next scheduled window, the reporter starts a background flush early and drops
 * additional events until space is available.
 *
 * <p>HTTP transport is deliberately pluggable. Applications provide a {@link ReactiveHttpClient}
 * implementation, usually backed by the HTTP client they already use. The default host is
 * {@code https://feddi.dev}; endpoint paths are fixed to
 * {@code /api/usage-proto/known-operation-hashes}, {@code /api/usage-proto/operations}, and
 * {@code /api/usage-proto/usage}.
 *
 * <p>Instances support concurrent {@code report(...)} calls. Call {@link #closeAsync()} or
 * {@link #close()} during shutdown to flush queued usage before the process exits.
 */
public final class ApiUsageReporter implements AutoCloseable {

    /**
     * Approximate compressed size of one usage event in bytes.
     */
    public static final int APPROX_COMPRESSED_USAGE_EVENT_BYTES = 45;

    /**
     * Default maximum number of pending invocations held in memory before new events are dropped.
     *
     * <p>The default is derived from a 1 MB compressed request budget and the observed approximate
     * compressed usage event size.
     */
    public static final int DEFAULT_MAX_QUEUE_SIZE = 1_000_000 / APPROX_COMPRESSED_USAGE_EVENT_BYTES;

    /**
     * Absolute maximum number of pending invocations held in memory.
     *
     * <p>The cap is derived from a 2 MB compressed request budget and an observed approximate
     * compressed usage event size of 45 bytes.
     */
    public static final int ABSOLUTE_MAX_QUEUE_SIZE = 2_000_000 / APPROX_COMPRESSED_USAGE_EVENT_BYTES;

    /**
     * Default lower bound for randomized background batch flush scheduling.
     */
    public static final Duration DEFAULT_BATCH_WINDOW_MIN = Duration.ofSeconds(20);

    /**
     * Default upper bound for randomized background batch flush scheduling.
     */
    public static final Duration DEFAULT_BATCH_WINDOW_MAX = Duration.ofSeconds(40);

    private final UsageReportClient client;
    private final ApiUsageDocumentAnalyzer analyzer;
    private final ConcurrentLinkedQueue<PendingUsage> pendingQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, OperationDefinition> pendingOperations = new ConcurrentHashMap<>();
    private final Set<String> knownOperationHashes = ConcurrentHashMap.newKeySet();
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final Object lifecycleLock = new Object();
    private final int maxQueueSize;
    private final Duration batchWindowMin;
    private final Duration batchWindowMax;
    private final ReporterScheduler scheduler;
    private final ReporterScheduler.Cancellable scheduledKnownOperationHashFetch;
    private final AtomicLong requestCounter = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Consumer<Throwable> flushErrorHandler;
    private final DoubleSupplier randomSupplier;
    private final boolean samplingEnabled;
    private volatile double sampleRate = 1.0;
    private volatile int multiplier = 1;
    private volatile boolean knownOperationHashesLoaded;
    private @Nullable Mono<Void> knownOperationHashesFetch;
    private @Nullable Throwable knownOperationHashesPrefetchFailure;
    private ReporterScheduler.Cancellable scheduledFlush;
    private boolean backgroundFlushInProgress;
    private boolean backgroundFlushAgainRequested;

    private ApiUsageReporter(Builder builder) {
        this.client = new UsageReportClient(
                builder.httpClient,
                builder.host,
                requireNonBlank(builder.feddiGraphVariantKey, "feddiGraphVariantKey")
        );
        this.analyzer = new ApiUsageDocumentAnalyzer();
        this.maxQueueSize = resolveMaxQueueSize(builder.maxQueueSize);
        this.batchWindowMin = builder.batchWindowMin;
        this.batchWindowMax = builder.batchWindowMax;
        this.flushErrorHandler = builder.flushErrorHandler != null ? builder.flushErrorHandler : ignored -> {};
        this.randomSupplier = builder.randomSupplier != null
                ? builder.randomSupplier
                : () -> ThreadLocalRandom.current().nextDouble();
        this.samplingEnabled = builder.samplingEnabled;
        this.scheduler = builder.scheduler != null ? builder.scheduler : new ScheduledExecutorReporterScheduler();

        if (builder.autoStart) {
            this.scheduledKnownOperationHashFetch = scheduler.schedule(
                    this::prefetchKnownOperationHashesSafely,
                    randomKnownOperationHashPrefetchDelay()
            );
            this.scheduledFlush = scheduler.schedule(this::processScheduledFlush, randomBatchWindow());
        } else {
            this.scheduledKnownOperationHashFetch = () -> {};
            this.scheduledFlush = null;
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
     * and returns immediately. The invocation may be sampled out when adaptive sampling is enabled,
     * or dropped if the reporter is closed or the queue is full.
     *
     * @param invocation completed API invocation to report
     * @return {@code true} when the invocation was queued, otherwise {@code false}
     */
    public boolean report(ApiUsageInvocation invocation) {
        Objects.requireNonNull(invocation, "invocation");
        boolean shouldStartFlush = false;
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return false;
            }

            requestCounter.incrementAndGet();
            if (samplingEnabled && randomSupplier.getAsDouble() >= sampleRate) {
                return false;
            }

            if (pendingCount.get() >= maxQueueSize) {
                droppedCount.incrementAndGet();
                return false;
            }

            pendingCount.incrementAndGet();
            pendingQueue.add(new PendingUsage(invocation, samplingEnabled ? multiplier : 1));
            if (pendingCount.get() >= maxQueueSize) {
                shouldStartFlush = requestBackgroundFlushSoonLocked();
            }
        }
        if (shouldStartFlush) {
            scheduler.execute(this::processBackgroundFlushSafely);
        }
        return true;
    }

    /**
     * Flushes all usage records that are queued when draining starts.
     *
     * <p>The returned {@link Mono} is cold; flushing starts when it is subscribed to. The first
     * flush fetches known operation hashes before draining queued records. Records queued while the
     * queue is being drained are left for a later flush. If no records are queued, the returned
     * response has {@code accepted == 0} after that initial cache load.
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
                scheduledKnownOperationHashFetch.cancel();
                cancelScheduledFlushLocked();
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

    private void processScheduledFlush() {
        boolean shouldStartFlush;
        synchronized (lifecycleLock) {
            scheduledFlush = null;
            shouldStartFlush = requestBackgroundFlushSoonLocked();
        }
        if (shouldStartFlush) {
            processBackgroundFlushSafely();
        }
    }

    private boolean requestBackgroundFlushSoonLocked() {
        if (closed.get()) {
            return false;
        }
        if (backgroundFlushInProgress) {
            backgroundFlushAgainRequested = true;
            return false;
        }
        backgroundFlushInProgress = true;
        cancelScheduledFlushLocked();
        return true;
    }

    private void processBackgroundFlushSafely() {
        processAndFlush()
                .subscribe(
                        ignored -> {},
                        error -> {
                            flushErrorHandler.accept(error);
                            completeBackgroundFlush(true);
                        },
                        () -> completeBackgroundFlush(false)
                );
    }

    private void completeBackgroundFlush(boolean failed) {
        boolean shouldStartFlush = false;
        synchronized (lifecycleLock) {
            backgroundFlushInProgress = false;
            boolean shouldDrainAgain = !failed
                    && pendingCount.get() > 0
                    && (pendingCount.get() >= maxQueueSize || backgroundFlushAgainRequested);
            backgroundFlushAgainRequested = false;
            if (closed.get()) {
                return;
            }
            if (shouldDrainAgain) {
                backgroundFlushInProgress = true;
                shouldStartFlush = true;
            } else {
                scheduleNextFlushLocked();
            }
        }
        if (shouldStartFlush) {
            scheduler.execute(this::processBackgroundFlushSafely);
        }
    }

    private void scheduleNextFlushLocked() {
        cancelScheduledFlushLocked();
        scheduledFlush = scheduler.schedule(this::processScheduledFlush, randomBatchWindow());
    }

    private void cancelScheduledFlushLocked() {
        if (scheduledFlush != null) {
            scheduledFlush.cancel();
            scheduledFlush = null;
        }
    }

    private void prefetchKnownOperationHashesSafely() {
        ensureKnownOperationHashesLoaded()
                .subscribe(
                        ignored -> {},
                        this::recordKnownOperationHashesPrefetchFailure
                );
    }

    private Mono<UsageReportResponse> processAndFlush() {
        Throwable prefetchFailure = consumeKnownOperationHashesPrefetchFailure();
        if (prefetchFailure != null) {
            return Mono.error(prefetchFailure);
        }
        return ensureKnownOperationHashesLoaded()
                .then(Mono.defer(this::processAndFlushAfterKnownOperationHashLoad));
    }

    private Mono<UsageReportResponse> processAndFlushAfterKnownOperationHashLoad() {
        recalculateSampling();

        var batch = drainQueue();
        if (batch.usageRequest().getEventsCount() == 0 && batch.operationDefinitions().isEmpty()) {
            return Mono.just(UsageReportResponse.newBuilder().setAccepted(0).build());
        }

        Mono<UsageReportResponse> registration = registerOperations(batch.operationDefinitions());
        if (batch.usageRequest().getEventsCount() == 0) {
            return registration;
        }

        return registration
                .onErrorResume(error -> {
                    flushErrorHandler.accept(error);
                    return Mono.just(UsageReportResponse.newBuilder().setAccepted(0).build());
                })
                .then(client.report(batch.usageRequest()));
    }

    private Mono<Void> ensureKnownOperationHashesLoaded() {
        synchronized (lifecycleLock) {
            if (knownOperationHashesLoaded) {
                return Mono.empty();
            }
            if (knownOperationHashesFetch == null) {
                knownOperationHashesFetch = client.getKnownOperationHashes()
                        .doOnNext(response -> knownOperationHashes.addAll(response.getOperationHashesList()))
                        .then()
                        .doOnSuccess(ignored -> markKnownOperationHashesLoaded())
                        .doOnError(ignored -> clearKnownOperationHashesFetch())
                        .cache();
            }
            return knownOperationHashesFetch;
        }
    }

    private void markKnownOperationHashesLoaded() {
        synchronized (lifecycleLock) {
            knownOperationHashesLoaded = true;
            knownOperationHashesFetch = null;
        }
    }

    private void clearKnownOperationHashesFetch() {
        synchronized (lifecycleLock) {
            if (!knownOperationHashesLoaded) {
                knownOperationHashesFetch = null;
            }
        }
    }

    private void recordKnownOperationHashesPrefetchFailure(Throwable error) {
        synchronized (lifecycleLock) {
            if (!knownOperationHashesLoaded) {
                knownOperationHashesPrefetchFailure = error;
            }
        }
    }

    private @Nullable Throwable consumeKnownOperationHashesPrefetchFailure() {
        synchronized (lifecycleLock) {
            Throwable error = knownOperationHashesPrefetchFailure;
            knownOperationHashesPrefetchFailure = null;
            return error;
        }
    }

    private void recalculateSampling() {
        long count = requestCounter.getAndSet(0);
        if (!samplingEnabled) {
            sampleRate = 1.0;
            multiplier = 1;
            return;
        }
        double rps = count / (batchWindowMax.toNanos() / 1_000_000_000.0);
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

    private DrainedBatch drainQueue() {
        int recordsToDrain = pendingCount.get();
        var events = new ArrayList<UsageEvent>(recordsToDrain);
        PendingUsage pending;
        for (int drained = 0; drained < recordsToDrain && (pending = pendingQueue.poll()) != null; drained++) {
            pendingCount.decrementAndGet();
            try {
                events.add(toUsageEvent(pending));
            } catch (RuntimeException e) {
                droppedCount.incrementAndGet();
                flushErrorHandler.accept(e);
            }
        }

        return new DrainedBatch(
                drainOperationDefinitions(),
                IngestUsageRequest.newBuilder()
                        .addAllEvents(events)
                        .build()
        );
    }

    private UsageEvent toUsageEvent(PendingUsage pending) {
        var invocation = pending.invocation();
        var usage = analyzer.analyze(invocation);
        String operationHash = sha256Hex(usage.canonicalDocument());
        if (!knownOperationHashes.contains(operationHash)) {
            pendingOperations.putIfAbsent(operationHash, toOperationDefinition(operationHash, usage));
        }

        var builder = UsageEvent.newBuilder()
                .setOperationHash(operationHash)
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

    private OperationDefinition toOperationDefinition(String operationHash, ApiUsageDocumentAnalyzer.ProcessedUsage usage) {
        return OperationDefinition.newBuilder()
                .setOperationHash(operationHash)
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
                .build();
    }

    private List<OperationDefinition> drainOperationDefinitions() {
        var operations = new ArrayList<OperationDefinition>(pendingOperations.size());
        for (var entry : pendingOperations.entrySet()) {
            String operationHash = entry.getKey();
            OperationDefinition operation = entry.getValue();
            if (knownOperationHashes.contains(operationHash)) {
                pendingOperations.remove(operationHash, operation);
                continue;
            }
            if (pendingOperations.remove(operationHash, operation)) {
                operations.add(operation);
            }
        }
        return operations;
    }

    private Mono<UsageReportResponse> registerOperations(List<OperationDefinition> operations) {
        if (operations.isEmpty()) {
            return Mono.just(UsageReportResponse.newBuilder().setAccepted(0).build());
        }

        var request = RegisterOperationsRequest.newBuilder()
                .addAllOperations(operations)
                .build();
        return client.registerOperations(request)
                .flatMap(response -> {
                    if (response.getAccepted() != operations.size()) {
                        return Mono.error(new UsageReportClientException(
                                "Operation registration accepted " + response.getAccepted()
                                        + " of " + operations.size() + " operation definitions"
                        ));
                    }
                    operations.forEach(operation -> knownOperationHashes.add(operation.getOperationHash()));
                    return Mono.just(response);
                })
                .doOnError(error -> operations.forEach(operation ->
                        pendingOperations.putIfAbsent(operation.getOperationHash(), operation)
                ));
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

    private static String sha256Hex(String input) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static String requireNonBlank(@Nullable String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static int resolveMaxQueueSize(int configuredMaxQueueSize) {
        int resolvedMaxQueueSize = configuredMaxQueueSize > 0
                ? configuredMaxQueueSize
                : DEFAULT_MAX_QUEUE_SIZE;
        if (resolvedMaxQueueSize > ABSOLUTE_MAX_QUEUE_SIZE) {
            throw new IllegalArgumentException(
                    "max queue size must not exceed " + ABSOLUTE_MAX_QUEUE_SIZE);
        }
        return resolvedMaxQueueSize;
    }

    private Duration randomKnownOperationHashPrefetchDelay() {
        return randomDuration(Duration.ZERO, batchWindowMin);
    }

    private Duration randomBatchWindow() {
        return randomDuration(batchWindowMin, batchWindowMax);
    }

    private Duration randomDuration(Duration min, Duration max) {
        long minNanos = min.toNanos();
        long maxNanos = max.toNanos();
        if (minNanos == maxNanos) {
            return min;
        }
        double random = Math.max(0.0, Math.min(randomSupplier.getAsDouble(), Math.nextDown(1.0)));
        return Duration.ofNanos(minNanos + (long) (random * (maxNanos - minNanos)));
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

    int getKnownOperationCacheSize() {
        return knownOperationHashes.size();
    }

    int getPendingOperationQueueSize() {
        return pendingOperations.size();
    }

    private record PendingUsage(ApiUsageInvocation invocation, int multiplier) {
    }

    private record DrainedBatch(List<OperationDefinition> operationDefinitions, IngestUsageRequest usageRequest) {
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
        private int maxQueueSize = DEFAULT_MAX_QUEUE_SIZE;
        private Duration batchWindowMin = DEFAULT_BATCH_WINDOW_MIN;
        private Duration batchWindowMax = DEFAULT_BATCH_WINDOW_MAX;
        private boolean autoStart = true;
        private boolean samplingEnabled = false;
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
         * slash, query, or fragment. The endpoint paths remain fixed to
         * {@code /api/usage-proto/known-operation-hashes}, {@code /api/usage-proto/operations},
         * and {@code /api/usage-proto/usage}.
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
         * Sets the maximum number of pending invocations held in memory.
         *
         * <p>When the queue is full, new usage events are dropped and
         * {@link ApiUsageReporter#report(ApiUsageInvocation)} returns {@code false}. Values less
         * than or equal to zero use {@link ApiUsageReporter#DEFAULT_MAX_QUEUE_SIZE}. Values above
         * {@link ApiUsageReporter#ABSOLUTE_MAX_QUEUE_SIZE} are rejected.
         *
         * <p>This setting bounds both memory retained by pending usage and the largest automatic
         * protobuf usage request, because each flush drains the whole queue that exists when
         * draining starts. It does not split a flush into smaller requests.
         *
         * @param maxQueueSize maximum pending queue size
         * @return this builder
         */
        public Builder maxQueueSize(int maxQueueSize) {
            if (maxQueueSize > ABSOLUTE_MAX_QUEUE_SIZE) {
                throw new IllegalArgumentException(
                        "max queue size must not exceed " + ABSOLUTE_MAX_QUEUE_SIZE);
            }
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        /**
         * Sets the randomized background batch flush window.
         *
         * <p>Each scheduled background flush is delayed by a new random duration in the inclusive
         * lower-bound and exclusive upper-bound range. Use the same value for both arguments when
         * deterministic fixed-delay scheduling is needed.
         *
         * <p>The window is time-based, not size-based. Records reported during the window accumulate
         * in the pending queue until a scheduled flush drains the queue. If the queue reaches
         * {@link ApiUsageReporter#DEFAULT_MAX_QUEUE_SIZE} or a custom {@link #maxQueueSize(int)}
         * before the timer fires, the reporter starts a background flush early. Smaller windows lower
         * expected batch size; larger windows increase expected batch size up to the configured queue
         * limit.
         *
         * @param min minimum delay between automatic flushes
         * @param max maximum delay between automatic flushes
         * @return this builder
         */
        public Builder batchWindow(Duration min, Duration max) {
            Objects.requireNonNull(min, "min");
            Objects.requireNonNull(max, "max");
            if (min.isZero() || min.isNegative()) {
                throw new IllegalArgumentException("min batch window must be positive");
            }
            if (max.isZero() || max.isNegative()) {
                throw new IllegalArgumentException("max batch window must be positive");
            }
            if (min.compareTo(max) > 0) {
                throw new IllegalArgumentException("min batch window must not exceed max batch window");
            }
            this.batchWindowMin = min;
            this.batchWindowMax = max;
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
         * Enables or disables adaptive usage sampling.
         *
         * <p>Sampling is disabled by default. When disabled, every accepted invocation is queued
         * with multiplier {@code 1}. Enabling sampling lets the reporter sample high-throughput
         * traffic and send aggregate multipliers.
         *
         * @param samplingEnabled whether adaptive sampling is enabled
         * @return this builder
         */
        public Builder samplingEnabled(boolean samplingEnabled) {
            this.samplingEnabled = samplingEnabled;
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
