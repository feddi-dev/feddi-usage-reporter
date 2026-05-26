package dev.feddi.api.usage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import java.util.concurrent.atomic.AtomicInteger;

import static dev.feddi.api.usage.ApiUsageReporterJcstressSupport.INVOCATION;
import static dev.feddi.api.usage.ApiUsageReporterJcstressSupport.REPORTS_PER_ACTOR;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

@JCStressTest
@State
@Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Every accepted report is sent exactly once and no records remain queued.")
@Outcome(expect = FORBIDDEN, desc = "Accepted records were lost, duplicated, or left queued.")
public class ApiUsageReporterConcurrentReportFlushCloseJCStress {

    private final ApiUsageReporterJcstressSupport.CountingReactiveHttpClient httpClient =
            new ApiUsageReporterJcstressSupport.CountingReactiveHttpClient();
    private final ApiUsageReporter reporter = ApiUsageReporterJcstressSupport.reporter(
            httpClient,
            REPORTS_PER_ACTOR * 4
    );
    private final AtomicInteger acceptedReports = new AtomicInteger();

    @Actor
    public void report1() {
        reportMany();
    }

    @Actor
    public void report2() {
        reportMany();
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
        ApiUsageReporterJcstressSupport.close(reporter);
        result.r1 = acceptedReports.get() - httpClient.acceptedCount();
        result.r2 = reporter.getPendingQueueSize();
    }

    private void reportMany() {
        for (int i = 0; i < REPORTS_PER_ACTOR; i++) {
            if (reporter.report(INVOCATION)) {
                acceptedReports.incrementAndGet();
            }
        }
    }
}
