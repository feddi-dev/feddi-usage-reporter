package dev.feddi.api.usage;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Objects;

public record UsageEvent(String graphVariantId, String operationName, Instant observedAt) {

    public UsageEvent {
        requireNonBlank(graphVariantId, "graphVariantId");
        requireNonBlank(operationName, "operationName");
        Objects.requireNonNull(observedAt, "observedAt");
    }

    public Timestamp observedAtTimestamp() {
        return Timestamp.newBuilder()
                .setSeconds(observedAt.getEpochSecond())
                .setNanos(observedAt.getNano())
                .build();
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
