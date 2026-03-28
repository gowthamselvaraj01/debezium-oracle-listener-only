package com.example.debeziumoracle.service;

import com.example.debeziumoracle.config.RetryDlqProperties;
import com.example.debeziumoracle.model.FailedEvent;
import com.example.debeziumoracle.model.RowChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class RetryableChangeProcessor {

    private static final Logger log = LoggerFactory.getLogger(RetryableChangeProcessor.class);

    private final BusinessChangeProcessor delegate;
    private final DeadLetterQueueService dlqService;
    private final RetryDlqProperties properties;

    public RetryableChangeProcessor(
            BusinessChangeProcessor delegate,
            DeadLetterQueueService dlqService,
            RetryDlqProperties properties
    ) {
        this.delegate = delegate;
        this.dlqService = dlqService;
        this.properties = properties;
    }

    public void processWithRetry(RowChange rowChange) {
        int attempt = 0;
        long backoff = properties.getInitialBackoffMs();
        Exception lastException = null;

        while (attempt <= properties.getMaxRetries()) {
            try {
                if (attempt > 0) {
                    log.info("Retry attempt {}/{} for table={}.{} op={}",
                            attempt, properties.getMaxRetries(),
                            rowChange.schemaName(), rowChange.tableName(),
                            rowChange.operation());
                }
                delegate.process(rowChange);
                return;
            } catch (Exception ex) {
                lastException = ex;
                attempt++;
                log.warn("Processing failed (attempt {}/{}): {}",
                        attempt, properties.getMaxRetries() + 1,
                        ex.getMessage());

                if (attempt <= properties.getMaxRetries()) {
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("Retry sleep interrupted, sending to DLQ");
                        break;
                    }
                    backoff = Math.min(
                            (long) (backoff * properties.getBackoffMultiplier()),
                            properties.getMaxBackoffMs()
                    );
                }
            }
        }

        log.error("All {} retries exhausted for table={}.{}, sending to DLQ",
                properties.getMaxRetries(), rowChange.schemaName(),
                rowChange.tableName());
        dlqService.enqueue(FailedEvent.of(rowChange, lastException, properties.getMaxRetries()));
    }

    public void replayFromDlq(String eventId) {
        FailedEvent failedEvent = dlqService.getById(eventId)
                .orElseThrow(() -> new IllegalArgumentException("DLQ event not found: " + eventId));

        log.info("Replaying DLQ event id={} table={}.{}",
                eventId, failedEvent.rowChange().schemaName(),
                failedEvent.rowChange().tableName());

        processWithRetry(failedEvent.rowChange());
        dlqService.remove(eventId);
    }

    public int replayAll() {
        var events = dlqService.getAll();
        int replayed = 0;
        for (FailedEvent event : events) {
            try {
                log.info("Replaying DLQ event id={}", event.id());
                processWithRetry(event.rowChange());
                dlqService.remove(event.id());
                replayed++;
            } catch (Exception ex) {
                log.error("Replay failed for event id={}: {}", event.id(), ex.getMessage());
            }
        }
        return replayed;
    }
}
