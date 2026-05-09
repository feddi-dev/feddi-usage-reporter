package dev.feddi.api.usage;

import graphql.language.Document;
import graphql.schema.GraphQLSchema;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ApiUsageInvocation {

    private final Document document;
    private final @Nullable String operationName;
    private final GraphQLSchema schema;
    private final long durationNanos;
    private final boolean httpError;
    private final boolean graphqlError;
    private final String clientName;
    private final String clientVersion;
    private final Instant timestamp;
    private final Map<String, @Nullable Object> variables;

    private ApiUsageInvocation(Builder builder) {
        this.document = Objects.requireNonNull(builder.document, "document");
        this.operationName = blankToNull(builder.operationName);
        this.schema = Objects.requireNonNull(builder.schema, "schema");
        this.durationNanos = builder.durationNanos;
        this.httpError = builder.httpError;
        this.graphqlError = builder.graphqlError;
        this.clientName = nullToEmpty(builder.clientName);
        this.clientVersion = nullToEmpty(builder.clientVersion);
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now();
        this.variables = immutableCopy(builder.variables);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Document document() {
        return document;
    }

    public @Nullable String operationName() {
        return operationName;
    }

    public GraphQLSchema schema() {
        return schema;
    }

    public long durationNanos() {
        return durationNanos;
    }

    public boolean httpError() {
        return httpError;
    }

    public boolean graphqlError() {
        return graphqlError;
    }

    public String clientName() {
        return clientName;
    }

    public String clientVersion() {
        return clientVersion;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public Map<String, @Nullable Object> variables() {
        return variables;
    }

    private static @Nullable String blankToNull(@Nullable String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String nullToEmpty(@Nullable String value) {
        return value == null ? "" : value;
    }

    private static Map<String, @Nullable Object> immutableCopy(@Nullable Map<String, @Nullable Object> variables) {
        if (variables == null || variables.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(variables));
    }

    public static final class Builder {

        private @Nullable Document document;
        private @Nullable String operationName;
        private @Nullable GraphQLSchema schema;
        private long durationNanos;
        private boolean httpError;
        private boolean graphqlError;
        private @Nullable String clientName;
        private @Nullable String clientVersion;
        private @Nullable Instant timestamp;
        private @Nullable Map<String, @Nullable Object> variables;

        private Builder() {
        }

        public Builder document(Document document) {
            this.document = document;
            return this;
        }

        public Builder operationName(@Nullable String operationName) {
            this.operationName = operationName;
            return this;
        }

        public Builder schema(GraphQLSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder durationNanos(long durationNanos) {
            this.durationNanos = durationNanos;
            return this;
        }

        public Builder httpError(boolean httpError) {
            this.httpError = httpError;
            return this;
        }

        public Builder graphqlError(boolean graphqlError) {
            this.graphqlError = graphqlError;
            return this;
        }

        public Builder clientName(@Nullable String clientName) {
            this.clientName = clientName;
            return this;
        }

        public Builder clientVersion(@Nullable String clientVersion) {
            this.clientVersion = clientVersion;
            return this;
        }

        public Builder timestamp(@Nullable Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder variables(@Nullable Map<String, @Nullable Object> variables) {
            this.variables = variables;
            return this;
        }

        public ApiUsageInvocation build() {
            return new ApiUsageInvocation(this);
        }
    }
}
