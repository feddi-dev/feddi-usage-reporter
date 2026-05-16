package dev.feddi.api.usage;

import java.time.Duration;

interface ReporterScheduler extends AutoCloseable {

    void execute(Runnable task);

    Cancellable schedule(Runnable task, Duration delay);

    Cancellable scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period);

    @Override
    void close();

    interface Cancellable {
        void cancel();
    }
}
