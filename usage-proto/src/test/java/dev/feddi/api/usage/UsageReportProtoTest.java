package dev.feddi.api.usage;

import com.google.protobuf.Timestamp;
import dev.feddi.api.usage.v1.InputUsageCoordinate;
import dev.feddi.api.usage.v1.InputUsageCoordinateKind;
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
                        .setCanonicalDocument("query GetUser { user { id name } }")
                        .addFieldCoordinates("Query.user")
                        .addFieldCoordinates("User.id")
                        .addInputUsageCoordinates(InputUsageCoordinate.newBuilder()
                                .setCoordinate("Query.user(id:)")
                                .setKind(InputUsageCoordinateKind.FIELD_ARGUMENT)
                                .build())
                        .addInputUsageCoordinates(InputUsageCoordinate.newBuilder()
                                .setCoordinate("UserFilter.status")
                                .setKind(InputUsageCoordinateKind.INPUT_OBJECT_FIELD)
                                .build())
                        .addInputUsageCoordinates(InputUsageCoordinate.newBuilder()
                                .setCoordinate("@include")
                                .setKind(InputUsageCoordinateKind.USED_DIRECTIVE)
                                .build())
                        .addInputUsageCoordinates(InputUsageCoordinate.newBuilder()
                                .setCoordinate("@include(if:)")
                                .setKind(InputUsageCoordinateKind.DIRECTIVE_ARGUMENT)
                                .build())
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
        assertEquals("query GetUser { user { id name } }", record.getCanonicalDocument());
        assertEquals("Query.user", record.getFieldCoordinates(0));
        assertEquals("User.id", record.getFieldCoordinates(1));
        assertEquals("Query.user(id:)", record.getInputUsageCoordinates(0).getCoordinate());
        assertEquals(InputUsageCoordinateKind.FIELD_ARGUMENT, record.getInputUsageCoordinates(0).getKind());
        assertEquals("UserFilter.status", record.getInputUsageCoordinates(1).getCoordinate());
        assertEquals(InputUsageCoordinateKind.INPUT_OBJECT_FIELD, record.getInputUsageCoordinates(1).getKind());
        assertEquals("@include", record.getInputUsageCoordinates(2).getCoordinate());
        assertEquals(InputUsageCoordinateKind.USED_DIRECTIVE, record.getInputUsageCoordinates(2).getKind());
        assertEquals("@include(if:)", record.getInputUsageCoordinates(3).getCoordinate());
        assertEquals(InputUsageCoordinateKind.DIRECTIVE_ARGUMENT, record.getInputUsageCoordinates(3).getKind());
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
