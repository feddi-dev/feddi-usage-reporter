package dev.feddi.api.usage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import java.util.concurrent.atomic.AtomicInteger;

import static dev.feddi.api.usage.ApiUsageReporterJcstressSupport.INVOCATION;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

@JCStressTest
@State
@Outcome(id = "2, 2", expect = ACCEPTABLE, desc = "Exactly two actors reserve the two queue slots.")
@Outcome(expect = FORBIDDEN, desc = "The queue size limit was exceeded or not fully used before rejection.")
public class ApiUsageReporterConcurrentManyActorQueueLimitJCStress {

    private final ApiUsageReporterJcstressSupport.CountingReactiveHttpClient httpClient =
            new ApiUsageReporterJcstressSupport.CountingReactiveHttpClient();
    private final ApiUsageReporter reporter = ApiUsageReporterJcstressSupport.reporter(httpClient, 2);
    private final AtomicInteger acceptedReports = new AtomicInteger();

    @Actor
    public void report1() {
        report();
    }

    @Actor
    public void report2() {
        report();
    }

    @Actor
    public void report3() {
        report();
    }

    @Actor
    public void report4() {
        report();
    }

    @Arbiter
    public void arbiter(II_Result result) {
        result.r1 = acceptedReports.get();
        result.r2 = reporter.getPendingQueueSize();
        reporter.close();
    }

    private void report() {
        if (reporter.report(INVOCATION)) {
            acceptedReports.incrementAndGet();
        }
    }
}
