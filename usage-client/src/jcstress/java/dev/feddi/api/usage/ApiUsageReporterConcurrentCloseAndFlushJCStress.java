package dev.feddi.api.usage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import static dev.feddi.api.usage.ApiUsageReporterJcstressSupport.INVOCATION;
import static dev.feddi.api.usage.ApiUsageReporterJcstressSupport.REPORTS_PER_ACTOR;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

@JCStressTest
@State
@Outcome(id = "1000, 0", expect = ACCEPTABLE, desc = "Concurrent final flushes send every queued report once.")
@Outcome(expect = FORBIDDEN, desc = "Records were lost, duplicated, or left queued.")
public class ApiUsageReporterConcurrentCloseAndFlushJCStress {

    private final ApiUsageReporterJcstressSupport.CountingReactiveHttpClient httpClient =
            new ApiUsageReporterJcstressSupport.CountingReactiveHttpClient();
    private final ApiUsageReporter reporter = ApiUsageReporterJcstressSupport.reporter(
            httpClient,
            REPORTS_PER_ACTOR * 2
    );

    public ApiUsageReporterConcurrentCloseAndFlushJCStress() {
        for (int i = 0; i < REPORTS_PER_ACTOR; i++) {
            if (!reporter.report(INVOCATION)) {
                throw new IllegalStateException("report was not queued");
            }
        }
    }

    @Actor
    public void flush() {
        ApiUsageReporterJcstressSupport.flush(reporter);
    }

    @Actor
    public void close() {
        ApiUsageReporterJcstressSupport.close(reporter);
    }

    @Arbiter
    public void arbiter(II_Result result) {
        ApiUsageReporterJcstressSupport.flush(reporter);
        result.r1 = httpClient.acceptedCount();
        result.r2 = reporter.getPendingQueueSize();
    }
}
