package dev.feddi.api.usage;

import com.google.protobuf.Timestamp;
import dev.feddi.api.usage.v1.IngestUsageRequest;
import dev.feddi.api.usage.v1.InputUsageCoordinate;
import dev.feddi.api.usage.v1.InputUsageCoordinateKind;
import dev.feddi.api.usage.v1.KnownOperationHashesResponse;
import dev.feddi.api.usage.v1.OperationDefinition;
import dev.feddi.api.usage.v1.RegisterOperationsRequest;
import dev.feddi.api.usage.v1.UsageEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsageReportProtoTest {

    @Test
    void registerOperationsRequest_roundTripsOperationDefinitionFields() throws Exception {
        var request = RegisterOperationsRequest.newBuilder()
                .addOperations(OperationDefinition.newBuilder()
                        .setOperationHash("abc123")
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
                        .build())
                .build();

        var operation = RegisterOperationsRequest.parseFrom(request.toByteArray()).getOperations(0);

        assertEquals("abc123", operation.getOperationHash());
        assertEquals("GetUser", operation.getOperationName());
        assertEquals("QUERY", operation.getOperationType());
        assertEquals("query GetUser { user { id name } }", operation.getCanonicalDocument());
        assertEquals("Query.user", operation.getFieldCoordinates(0));
        assertEquals("User.id", operation.getFieldCoordinates(1));
        assertEquals("Query.user(id:)", operation.getInputUsageCoordinates(0).getCoordinate());
        assertEquals(InputUsageCoordinateKind.FIELD_ARGUMENT, operation.getInputUsageCoordinates(0).getKind());
        assertEquals("UserFilter.status", operation.getInputUsageCoordinates(1).getCoordinate());
        assertEquals(InputUsageCoordinateKind.INPUT_OBJECT_FIELD, operation.getInputUsageCoordinates(1).getKind());
        assertEquals("@include", operation.getInputUsageCoordinates(2).getCoordinate());
        assertEquals(InputUsageCoordinateKind.USED_DIRECTIVE, operation.getInputUsageCoordinates(2).getKind());
        assertEquals("@include(if:)", operation.getInputUsageCoordinates(3).getCoordinate());
        assertEquals(InputUsageCoordinateKind.DIRECTIVE_ARGUMENT, operation.getInputUsageCoordinates(3).getKind());
    }

    @Test
    void ingestUsageRequest_roundTripsHashOnlyUsageFields() throws Exception {
        var observedAt = Instant.parse("2026-03-22T10:00:00.123456789Z");
        var request = IngestUsageRequest.newBuilder()
                .addEvents(UsageEvent.newBuilder()
                        .setOperationHash("abc123")
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

        var event = IngestUsageRequest.parseFrom(request.toByteArray()).getEvents(0);

        assertEquals("abc123", event.getOperationHash());
        assertEquals(1_500_000, event.getDurationNanos());
        assertTrue(event.getGraphqlError());
        assertEquals("web-app", event.getClientName());
        assertEquals("1.2.0", event.getClientVersion());
        assertEquals(observedAt.getEpochSecond(), event.getTimestamp().getSeconds());
        assertEquals(observedAt.getNano(), event.getTimestamp().getNanos());
        assertTrue(event.hasMultiplier());
        assertEquals(10, event.getMultiplier());
    }

    @Test
    void knownOperationHashesResponse_roundTripsOperationHashes() throws Exception {
        var response = KnownOperationHashesResponse.newBuilder()
                .addOperationHashes("hash-a")
                .addOperationHashes("hash-b")
                .build();

        var parsed = KnownOperationHashesResponse.parseFrom(response.toByteArray());

        assertEquals(List.of("hash-a", "hash-b"), parsed.getOperationHashesList());
    }
}
