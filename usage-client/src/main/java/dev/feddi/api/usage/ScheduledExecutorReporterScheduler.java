package dev.feddi.api.usage;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class ScheduledExecutorReporterScheduler implements ReporterScheduler {

    private final ScheduledExecutorService delegate;

    ScheduledExecutorReporterScheduler() {
        this.delegate = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "feddi-api-usage-reporter");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void execute(Runnable task) {
        delegate.execute(task);
    }

    @Override
    public Cancellable schedule(Runnable task, Duration delay) {
        var future = delegate.schedule(task, delay.toNanos(), TimeUnit.NANOSECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public void close() {
        delegate.shutdown();
    }
}
