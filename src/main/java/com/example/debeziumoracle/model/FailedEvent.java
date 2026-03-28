package com.example.debeziumoracle.model;

import java.time.Instant;
import java.util.UUID;

public record FailedEvent(
        String id,
        RowChange rowChange,
        String errorMessage,
        String errorClass,
        int retryCount,
        Instant failedAt
) {

    public static FailedEvent of(RowChange rowChange, Exception error, int retryCount) {
        return new FailedEvent(
                UUID.randomUUID().toString(),
                rowChange,
                error.getMessage(),
                error.getClass().getName(),
                retryCount,
                Instant.now()
        );
    }
}
