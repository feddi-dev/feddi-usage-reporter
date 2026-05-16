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
@Outcome(id = "2000, 0", expect = ACCEPTABLE, desc = "Every queued report is sent exactly once.")
@Outcome(expect = FORBIDDEN, desc = "Records were lost, duplicated, or left queued.")
public class ApiUsageReporterConcurrentFlushJCStress {

    private final ApiUsageReporterJcstressSupport.CountingReactiveHttpClient httpClient =
            new ApiUsageReporterJcstressSupport.CountingReactiveHttpClient();
    private final ApiUsageReporter reporter = ApiUsageReporterJcstressSupport.reporter(
            httpClient,
            REPORTS_PER_ACTOR * 4
    );

    private volatile boolean report1Done;
    private volatile boolean report2Done;

    @Actor
    public void report1() {
        reportMany();
        report1Done = true;
    }

    @Actor
    public void report2() {
        reportMany();
        report2Done = true;
    }

    @Actor
    public void flush1() {
        flushUntilReportsComplete();
    }

    @Actor
    public void flush2() {
        flushUntilReportsComplete();
    }

    @Arbiter
    public void arbiter(II_Result result) {
        ApiUsageReporterJcstressSupport.flush(reporter);
        result.r1 = httpClient.acceptedCount();
        result.r2 = reporter.getPendingQueueSize();
        reporter.close();
    }

    private void reportMany() {
        for (int i = 0; i < REPORTS_PER_ACTOR; i++) {
            if (!reporter.report(INVOCATION)) {
                throw new IllegalStateException("report was not queued");
            }
        }
    }

    private void flushUntilReportsComplete() {
        while (!report1Done || !report2Done) {
            ApiUsageReporterJcstressSupport.flush(reporter);
        }
        ApiUsageReporterJcstressSupport.flush(reporter);
    }
}
