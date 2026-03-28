package com.example.debeziumoracle.service;

import com.example.debeziumoracle.config.RetryDlqProperties;
import com.example.debeziumoracle.model.FailedEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class DeadLetterQueueService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueService.class);
    private static final String DLQ_FILE_NAME = "dlq-events.json";

    private final RetryDlqProperties properties;
    private final ObjectMapper objectMapper;
    private final List<FailedEvent> failedEvents = new CopyOnWriteArrayList<>();
    private Path dlqFile;

    public DeadLetterQueueService(RetryDlqProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws IOException {
        Path dlqDir = Path.of(properties.getDlqFilePath()).toAbsolutePath();
        Files.createDirectories(dlqDir);
        dlqFile = dlqDir.resolve(DLQ_FILE_NAME);
        loadFromDisk();
    }

    public void enqueue(FailedEvent event) {
        if (failedEvents.size() >= properties.getDlqMaxSize()) {
            log.warn("DLQ max size ({}) reached, removing oldest event", properties.getDlqMaxSize());
            failedEvents.remove(0);
        }
        failedEvents.add(event);
        persistToDisk();
        log.info("Event added to DLQ: id={} table={}.{} error={}",
                event.id(), event.rowChange().schemaName(),
                event.rowChange().tableName(), event.errorMessage());
    }

    public List<FailedEvent> getAll() {
        return Collections.unmodifiableList(failedEvents);
    }

    public Optional<FailedEvent> getById(String id) {
        return failedEvents.stream()
                .filter(e -> e.id().equals(id))
                .findFirst();
    }

    public boolean remove(String id) {
        boolean removed = failedEvents.removeIf(e -> e.id().equals(id));
        if (removed) {
            persistToDisk();
        }
        return removed;
    }

    public int clear() {
        int size = failedEvents.size();
        failedEvents.clear();
        persistToDisk();
        return size;
    }

    public int size() {
        return failedEvents.size();
    }

    private void persistToDisk() {
        try {
            objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValue(dlqFile.toFile(), new ArrayList<>(failedEvents));
        } catch (IOException ex) {
            log.error("Failed to persist DLQ to disk", ex);
        }
    }

    private void loadFromDisk() {
        if (Files.exists(dlqFile)) {
            try {
                long fileSize = Files.size(dlqFile);
                if (fileSize == 0) {
                    return;
                }
                List<FailedEvent> loaded = objectMapper.readValue(
                        dlqFile.toFile(),
                        new TypeReference<List<FailedEvent>>() {}
                );
                failedEvents.addAll(loaded);
                log.info("Loaded {} events from DLQ file", loaded.size());
            } catch (IOException ex) {
                log.error("Failed to load DLQ from disk", ex);
            }
        }
    }
}
