package dev.feddi.api.usage;

import java.time.Duration;

interface ReporterScheduler extends AutoCloseable {

    void execute(Runnable task);

    Cancellable schedule(Runnable task, Duration delay);

    @Override
    void close();

    interface Cancellable {
        void cancel();
    }
}
