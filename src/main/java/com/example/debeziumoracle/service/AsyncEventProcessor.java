package com.example.debeziumoracle.service;

import com.example.debeziumoracle.config.AsyncProcessingProperties;
import com.example.debeziumoracle.model.RowChange;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class AsyncEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(AsyncEventProcessor.class);

    private final AsyncProcessingProperties properties;
    private final RetryableChangeProcessor retryableProcessor;

    private ThreadPoolExecutor executor;
    private final AtomicLong submittedCount = new AtomicLong(0);
    private final AtomicLong completedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong rejectedCount = new AtomicLong(0);

    public AsyncEventProcessor(AsyncProcessingProperties properties,
                               RetryableChangeProcessor retryableProcessor) {
        this.properties = properties;
        this.retryableProcessor = retryableProcessor;
    }

    @PostConstruct
    public void init() {
        if (!properties.isEnabled()) {
            log.info("Async processing is disabled, events will be processed synchronously");
            return;
        }

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, properties.getThreadNamePrefix() + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };

        RejectedExecutionHandler rejectionHandler = (runnable, poolExecutor) -> {
            rejectedCount.incrementAndGet();
            log.error("Async event processing rejected — queue full (capacity={}). " +
                            "Consider increasing app.async.queue-capacity or app.async.max-pool-size.",
                    properties.getQueueCapacity());
        };

        executor = new ThreadPoolExecutor(
                properties.getCorePoolSize(),
                properties.getMaxPoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(properties.getQueueCapacity()),
                threadFactory,
                rejectionHandler
        );

        log.info("Async event processor initialized: corePool={}, maxPool={}, queueCapacity={}",
                properties.getCorePoolSize(), properties.getMaxPoolSize(),
                properties.getQueueCapacity());
    }

    public void submitEvent(RowChange rowChange) {
        if (!properties.isEnabled() || executor == null) {
            retryableProcessor.processWithRetry(rowChange);
            return;
        }

        submittedCount.incrementAndGet();
        executor.submit(() -> {
            try {
                boolean success = retryableProcessor.processWithRetry(rowChange);
                if (success) {
                    completedCount.incrementAndGet();
                } else {
                    failedCount.incrementAndGet();
                }
            } catch (Exception ex) {
                failedCount.incrementAndGet();
                log.error("Unexpected error in async event processing for table={}.{}: {}",
                        rowChange.schemaName(), rowChange.tableName(), ex.getMessage(), ex);
            }
        });
    }

    public long getSubmittedCount() {
        return submittedCount.get();
    }

    public long getCompletedCount() {
        return completedCount.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }

    public long getRejectedCount() {
        return rejectedCount.get();
    }

    public int getActiveThreads() {
        return executor != null ? executor.getActiveCount() : 0;
    }

    public int getQueueSize() {
        return executor != null ? executor.getQueue().size() : 0;
    }

    public int getPoolSize() {
        return executor != null ? executor.getPoolSize() : 0;
    }

    @PreDestroy
    public void shutdown() {
        if (executor != null) {
            log.info("Shutting down async event processor, waiting for {} queued events...",
                    executor.getQueue().size());
            executor.shutdown();
            try {
                if (!executor.awaitTermination(properties.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                    log.warn("Async processor did not terminate in {}s, forcing shutdown",
                            properties.getShutdownTimeoutSeconds());
                    executor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
            log.info("Async event processor shut down. submitted={}, completed={}, failed={}, rejected={}",
                    submittedCount.get(), completedCount.get(), failedCount.get(), rejectedCount.get());
        }
    }
}
