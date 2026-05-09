# feddi API Usage Client

Java 21 modules for sending protobuf usage reports to the feddi Platform.

## Modules

- `usage-proto`: protobuf schema and generated Java contract classes.
- `usage-client`: GraphQL Java based API usage reporter, reactive client facade,
  and pluggable reactive HTTP transport API.

The backend depends only on `usage-proto`. Applications should depend on
`usage-client`, provide their own `ReactiveHttpClient` implementation, and
configure `ApiUsageReporter` with a Feddi graph variant key via
`feddiGraphVariantKey(...)`. The reporter sends to
`https://feddi.dev/api/usage-proto` by default. Tests and self-hosted
deployments can override the host, but the endpoint path is always
`/api/usage-proto`.

`ApiUsageReporter` receives a GraphQL Java `Document`, operation name, and
`GraphQLSchema` for each completed API call. It generates the canonical
operation document with `AstSignature`, extracts field coordinates, field
argument coordinates, and input object field coordinates, samples
high-throughput traffic, and periodically flushes protobuf batches to the
feddi Platform.

## Usage Guide

Add the usage client to the API process that executes GraphQL requests:

```groovy
dependencies {
    implementation 'dev.feddi:feddi-api-usage-client:0.1.0-SNAPSHOT'
}
```

The library does not bundle an HTTP implementation. Provide a
`ReactiveHttpClient` adapter for the HTTP client already used by the host
application. For example, with Spring `WebClient`:

```java
import dev.feddi.api.usage.http.ReactiveHttpClient;
import dev.feddi.api.usage.http.ReactiveHttpRequest;
import dev.feddi.api.usage.http.ReactiveHttpResponse;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

final class WebClientReactiveHttpClient implements ReactiveHttpClient {

    private final WebClient webClient = WebClient.builder().build();

    @Override
    public Mono<ReactiveHttpResponse> exchange(ReactiveHttpRequest request) {
        var spec = webClient
                .method(HttpMethod.valueOf(request.method()))
                .uri(request.uri());

        request.headers().forEach((name, values) ->
                values.forEach(value -> spec.header(name, value)));

        return spec.body(request.body(), byte[].class)
                .exchangeToMono(response -> response.bodyToMono(byte[].class)
                        .defaultIfEmpty(new byte[0])
                        .map(body -> new ReactiveHttpResponse(
                                response.statusCode().value(),
                                response.headers().asHttpHeaders(),
                                Mono.just(body))));
    }
}
```

Create one reporter for the process and reuse it for all requests that belong
to the configured graph variant:

```java
import dev.feddi.api.usage.ApiUsageReporter;

var reporter = ApiUsageReporter.builder(new WebClientReactiveHttpClient())
        .feddiGraphVariantKey(System.getenv("FEDDI_GRAPH_VARIANT_KEY"))
        .build();
```

The default host is `https://feddi.dev`, and the endpoint path is always
`/api/usage-proto`. Self-hosted deployments and tests can override only the
host:

```java
var reporter = ApiUsageReporter.builder(httpClient)
        .feddiGraphVariantKey(feddiGraphVariantKey)
        .host("https://platform.example.com")
        .build();
```

Report each completed GraphQL request after execution. The `Document` should be
the parsed GraphQL Java document for the request, and the `GraphQLSchema`
should be the executable schema used to run it.

Pass the runtime variables map when the operation used variables. This lets the
reporter detect which optional input object fields were actually present in the
request. Inline input object literals are analyzed from the GraphQL document.

```java
import dev.feddi.api.usage.ApiUsageInvocation;

long startedAt = System.nanoTime();

// Execute the GraphQL request with your GraphQL Java runtime.

boolean queued = reporter.report(ApiUsageInvocation.builder()
        .document(document)
        .operationName(operationName)
        .schema(graphQLSchema)
        .variables(variables)
        .durationNanos(System.nanoTime() - startedAt)
        .httpError(httpStatusCode >= 500)
        .graphqlError(!executionResult.getErrors().isEmpty())
        .clientName("orders-api")
        .clientVersion("1.0.0")
        .build());
```

`report(...)` is non-blocking. It returns `false` when the reporter is closed,
the event was sampled out, or the in-memory queue is full.

Flush and close the reporter during application shutdown:

```java
reporter.close();
```

Reactive hosts can use `closeAsync()` instead:

```java
reporter.closeAsync().subscribe();
```

### Configuration

- `feddiGraphVariantKey(...)` is required and is used as the bearer token.
- `host(...)` is optional. It must be an absolute host URI with no path other
  than an optional trailing slash, query, or fragment.
- `flushInterval(...)` controls the scheduled background flush interval.
- `maxBatchSize(...)` controls the maximum protobuf records per request.
- `maxQueueSize(...)` controls the pending in-memory queue size.
- `flushErrorHandler(...)` receives background flush failures and per-record
  analysis failures.

Sampling is recalculated on every flush from the request count observed during
the flush interval. Traffic below 100 requests per second sends every event.
Higher traffic is sampled and sent with a multiplier so aggregate counts remain
representative.
