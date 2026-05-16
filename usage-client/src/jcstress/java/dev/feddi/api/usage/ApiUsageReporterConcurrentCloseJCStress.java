package dev.feddi.api.usage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

import static dev.feddi.api.usage.ApiUsageReporterJcstressSupport.INVOCATION;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

@JCStressTest
@State
@Outcome(id = "0, 0, 0", expect = ACCEPTABLE, desc = "Close wins and the report is rejected.")
@Outcome(id = "1, 1, 0", expect = ACCEPTABLE, desc = "Report wins and close flushes it.")
@Outcome(expect = FORBIDDEN, desc = "An accepted report was lost or left queued after close.")
public class ApiUsageReporterConcurrentCloseJCStress {

    private final ApiUsageReporterJcstressSupport.CountingReactiveHttpClient httpClient =
            new ApiUsageReporterJcstressSupport.CountingReactiveHttpClient();
    private final ApiUsageReporter reporter = ApiUsageReporterJcstressSupport.reporter(httpClient, 100);

    private int reportResult;

    @Actor
    public void report() {
        reportResult = reporter.report(INVOCATION) ? 1 : 0;
    }

    @Actor
    public void close() {
        ApiUsageReporterJcstressSupport.close(reporter);
    }

    @Arbiter
    public void arbiter(III_Result result) {
        result.r1 = reportResult;
        result.r2 = httpClient.acceptedCount();
        result.r3 = reporter.getPendingQueueSize();
    }
}
