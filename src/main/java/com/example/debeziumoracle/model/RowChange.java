package com.example.debeziumoracle.model;

import java.time.Instant;
import java.util.Map;

public record RowChange(
        ChangeSource source,
        String schemaName,
        String tableName,
        String operation,
        String rowId,
        Map<String, Object> before,
        Map<String, Object> after,
        Instant capturedAt
) {
}
