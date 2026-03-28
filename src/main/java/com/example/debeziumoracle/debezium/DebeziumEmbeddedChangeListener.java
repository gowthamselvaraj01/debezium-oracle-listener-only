package com.example.debeziumoracle.debezium;

import com.example.debeziumoracle.config.DebeziumCaptureProperties;
import com.example.debeziumoracle.model.ChangeSource;
import com.example.debeziumoracle.model.RowChange;
import com.example.debeziumoracle.service.RetryableChangeProcessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class DebeziumEmbeddedChangeListener {

    private static final Logger log = LoggerFactory.getLogger(DebeziumEmbeddedChangeListener.class);

    private final DebeziumCaptureProperties properties;
    private final RetryableChangeProcessor processor;
    private final ObjectMapper objectMapper;

    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executorService;

    public DebeziumEmbeddedChangeListener(
            DebeziumCaptureProperties properties,
            RetryableChangeProcessor processor,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.processor = processor;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void start() throws IOException {
        prepareFile(properties.getOffsetFileName());
        prepareFile(properties.getSchemaHistoryFileName());

        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", properties.getConnectorName());
        connectorProperties.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
        connectorProperties.setProperty("database.hostname", properties.getDatabaseHostname());
        connectorProperties.setProperty("database.port", String.valueOf(properties.getDatabasePort()));
        connectorProperties.setProperty("database.user", properties.getDatabaseUser());
        connectorProperties.setProperty("database.password", properties.getDatabasePassword());
        connectorProperties.setProperty("database.dbname", properties.getDatabaseDbname());
        //connectorProperties.setProperty("database.pdb.name", properties.getDatabasePdbName());
        connectorProperties.setProperty("schema.include.list", properties.getSchemaIncludeList());
        connectorProperties.setProperty("table.include.list", properties.getTableIncludeList());
        connectorProperties.setProperty("topic.prefix", properties.getTopicPrefix());
        connectorProperties.setProperty("snapshot.mode", "initial");
        connectorProperties.setProperty("log.mining.strategy", "online_catalog");
        connectorProperties.setProperty("include.schema.changes", "false");
        connectorProperties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        connectorProperties.setProperty("offset.storage.file.filename", properties.getOffsetFileName());
        connectorProperties.setProperty("offset.flush.interval.ms", "1000");
        connectorProperties.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        connectorProperties.setProperty("schema.history.internal.file.filename", properties.getSchemaHistoryFileName());

        engine = DebeziumEngine.create(Json.class)
                .using(connectorProperties)
                .notifying(this::handleChangeEvent)
                .build();

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(engine);
        log.info("Debezium embedded engine started for {}", properties.getTableIncludeList());
    }

    private void handleChangeEvent(ChangeEvent<String, String> event) {
        try {
            if (event.value() == null) {
                return;
            }

            JsonNode root = objectMapper.readTree(event.value());
            JsonNode payload = root.path("payload");
            String operation = payload.path("op").asText();

            Map<String, Object> before = toMap(payload.get("before"));
            Map<String, Object> after = toMap(payload.get("after"));
            Map<String, Object> source = toMap(payload.get("source"));

            RowChange rowChange = new RowChange(
                    ChangeSource.DEBEZIUM,
                    stringValue(source.get("schema")),
                    stringValue(source.get("table")),
                    operation,
                    event.key(),
                    before,
                    after,
                    Instant.now()
            );
            processor.processWithRetry(rowChange);
        } catch (Exception ex) {
            log.error("Failed to process Debezium event", ex);
        }
    }

    private Map<String, Object> toMap(JsonNode node) {
        if (node == null || node.isNull()) {
            return Map.of();
        }
        return objectMapper.convertValue(node, new TypeReference<>() {
        });
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private void prepareFile(String fileName) throws IOException {
        Path path = Path.of(fileName).toAbsolutePath();
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (Files.notExists(path)) {
            Files.createFile(path);
        }
    }

    @PreDestroy
    public void stop() throws Exception {
        if (engine != null) {
            engine.close();
        }
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
