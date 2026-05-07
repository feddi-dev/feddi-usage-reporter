package dev.feddi.api.usage;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import reactor.core.publisher.Mono;

import java.util.Objects;

public final class UsageEventEncoder {

    public Mono<ByteString> encode(UsageEvent event) {
        return toStruct(event).map(Struct::toByteString);
    }

    public Mono<Struct> toStruct(UsageEvent event) {
        Objects.requireNonNull(event, "event");
        return Mono.fromSupplier(() -> {
            var timestamp = event.observedAtTimestamp();
            var timestampStruct = Struct.newBuilder()
                    .putFields("seconds", numberValue(timestamp.getSeconds()))
                    .putFields("nanos", numberValue(timestamp.getNanos()))
                    .build();

            return Struct.newBuilder()
                    .putFields("graphVariantId", stringValue(event.graphVariantId()))
                    .putFields("operationName", stringValue(event.operationName()))
                    .putFields("observedAt", Value.newBuilder().setStructValue(timestampStruct).build())
                    .build();
        });
    }

    private static Value stringValue(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value numberValue(double value) {
        return Value.newBuilder().setNumberValue(value).build();
    }
}
