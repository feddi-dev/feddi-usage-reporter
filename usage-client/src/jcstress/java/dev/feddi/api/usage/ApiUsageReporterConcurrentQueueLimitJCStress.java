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
@Outcome(id = "1, 0, 1", expect = ACCEPTABLE, desc = "First actor reserves the only queue slot.")
@Outcome(id = "0, 1, 1", expect = ACCEPTABLE, desc = "Second actor reserves the only queue slot.")
@Outcome(expect = FORBIDDEN, desc = "Queue size limit was exceeded or no report was accepted.")
public class ApiUsageReporterConcurrentQueueLimitJCStress {

    private final ApiUsageReporterJcstressSupport.CountingReactiveHttpClient httpClient =
            new ApiUsageReporterJcstressSupport.CountingReactiveHttpClient();
    private final ApiUsageReporter reporter = ApiUsageReporterJcstressSupport.reporter(httpClient, 1);

    private int report1Result;
    private int report2Result;

    @Actor
    public void report1() {
        report1Result = reporter.report(INVOCATION) ? 1 : 0;
    }

    @Actor
    public void report2() {
        report2Result = reporter.report(INVOCATION) ? 1 : 0;
    }

    @Arbiter
    public void arbiter(III_Result result) {
        result.r1 = report1Result;
        result.r2 = report2Result;
        result.r3 = reporter.getPendingQueueSize();
        reporter.close();
    }
}
