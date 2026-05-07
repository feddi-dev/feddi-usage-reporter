package dev.feddi.api.usage;

import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class UsageEventEncoderTest {

    @Test
    void convertsEventToProtobufStruct() {
        var event = new UsageEvent(
                "variant-1",
                "ProductQuery",
                Instant.ofEpochSecond(1_700_000_000L, 123_000_000));

        StepVerifier.create(new UsageEventEncoder().toStruct(event))
                .assertNext(struct -> {
                    assertEquals("variant-1", stringField(struct, "graphVariantId"));
                    assertEquals("ProductQuery", stringField(struct, "operationName"));

                    var timestamp = struct.getFieldsOrThrow("observedAt").getStructValue();
                    assertEquals(1_700_000_000d, timestamp.getFieldsOrThrow("seconds").getNumberValue());
                    assertEquals(123_000_000d, timestamp.getFieldsOrThrow("nanos").getNumberValue());
                })
                .verifyComplete();
    }

    @Test
    void encodesEventToNonEmptyProtobufBytes() {
        var event = new UsageEvent("variant-1", "ProductQuery", Instant.EPOCH);

        StepVerifier.create(new UsageEventEncoder().encode(event))
                .assertNext(bytes -> assertFalse(bytes.isEmpty()))
                .verifyComplete();
    }

    private static String stringField(Struct struct, String fieldName) {
        return struct.getFieldsOrThrow(fieldName).getStringValue();
    }
}
