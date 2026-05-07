package dev.feddi.api.usage;

import com.google.protobuf.Timestamp;
import dev.feddi.api.usage.v1.UsageRecord;
import dev.feddi.api.usage.v1.UsageReportRequest;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsageReportProtoTest {

    @Test
    void usageReportRequest_roundTripsAllCurrentUsageFields() throws Exception {
        var observedAt = Instant.parse("2026-03-22T10:00:00.123456789Z");
        var request = UsageReportRequest.newBuilder()
                .addRecords(UsageRecord.newBuilder()
                        .setOperationName("GetUser")
                        .setOperationType("QUERY")
                        .setOperationHash("abc123")
                        .setOperationBody("query GetUser { user { id name } }")
                        .addFieldCoordinates("Query.user")
                        .addFieldCoordinates("User.id")
                        .setDurationNanos(1_500_000)
                        .setHttpError(false)
                        .setGraphqlError(true)
                        .setClientName("web-app")
                        .setClientVersion("1.2.0")
                        .setTimestamp(Timestamp.newBuilder()
                                .setSeconds(observedAt.getEpochSecond())
                                .setNanos(observedAt.getNano())
                                .build())
                        .setMultiplier(10)
                        .build())
                .build();

        var parsed = UsageReportRequest.parseFrom(request.toByteArray());
        var record = parsed.getRecords(0);

        assertEquals("GetUser", record.getOperationName());
        assertEquals("QUERY", record.getOperationType());
        assertEquals("abc123", record.getOperationHash());
        assertEquals("query GetUser { user { id name } }", record.getOperationBody());
        assertEquals("Query.user", record.getFieldCoordinates(0));
        assertEquals("User.id", record.getFieldCoordinates(1));
        assertEquals(1_500_000, record.getDurationNanos());
        assertTrue(record.getGraphqlError());
        assertEquals("web-app", record.getClientName());
        assertEquals("1.2.0", record.getClientVersion());
        assertEquals(observedAt.getEpochSecond(), record.getTimestamp().getSeconds());
        assertEquals(observedAt.getNano(), record.getTimestamp().getNanos());
        assertTrue(record.hasMultiplier());
        assertEquals(10, record.getMultiplier());
    }
}
