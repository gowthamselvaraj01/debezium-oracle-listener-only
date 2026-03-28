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

    /**
     * Attempts to process the row change with retries and exponential backoff.
     *
     * @return {@code true} if processing succeeded, {@code false} if all retries
     *         were exhausted and the event was sent to the DLQ.
     */
    public boolean processWithRetry(RowChange rowChange) {
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
                return true;
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
        return false;
    }

    /**
     * Replays a single event from the DLQ. Removes it only if processing succeeds.
     *
     * @return {@code true} if replay succeeded, {@code false} if it failed again.
     */
    public boolean replayFromDlq(String eventId) {
        FailedEvent failedEvent = dlqService.getById(eventId)
                .orElseThrow(() -> new IllegalArgumentException("DLQ event not found: " + eventId));

        log.info("Replaying DLQ event id={} table={}.{}",
                eventId, failedEvent.rowChange().schemaName(),
                failedEvent.rowChange().tableName());

        boolean success = processWithRetry(failedEvent.rowChange());
        if (success) {
            dlqService.remove(eventId);
        }
        return success;
    }

    public int replayAll() {
        var events = dlqService.getAll();
        int replayed = 0;
        for (FailedEvent event : events) {
            log.info("Replaying DLQ event id={}", event.id());
            boolean success = processWithRetry(event.rowChange());
            if (success) {
                dlqService.remove(event.id());
                replayed++;
            } else {
                log.warn("Replay failed again for event id={}, keeping in DLQ", event.id());
            }
        }
        return replayed;
    }
}
