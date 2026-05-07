package dev.feddi.api.usage;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UsageEventTest {

    @Test
    void exposesObservedAtAsProtobufTimestamp() {
        var event = new UsageEvent("variant-1", "ProductQuery", Instant.ofEpochSecond(42, 7));

        var timestamp = event.observedAtTimestamp();

        assertEquals(42, timestamp.getSeconds());
        assertEquals(7, timestamp.getNanos());
    }

    @Test
    void rejectsBlankRequiredFields() {
        assertThrows(IllegalArgumentException.class, () -> new UsageEvent("", "ProductQuery", Instant.EPOCH));
        assertThrows(IllegalArgumentException.class, () -> new UsageEvent("variant-1", " ", Instant.EPOCH));
    }
}
