package com.example.debeziumoracle.debezium;

import com.example.debeziumoracle.model.ChangeSource;
import com.example.debeziumoracle.model.RowChange;
import com.example.debeziumoracle.service.RetryableChangeProcessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DebeziumKafkaChangeListener {

    private static final Logger log = LoggerFactory.getLogger(DebeziumKafkaChangeListener.class);
    private static final int NUM_PROCESSOR_THREADS = 8;

    private final RetryableChangeProcessor processor;
    private final ObjectMapper objectMapper;
    private final ExecutorService eventProcessorExecutor;

    public DebeziumKafkaChangeListener(RetryableChangeProcessor processor, ObjectMapper objectMapper) {
        this.processor = processor;
        this.objectMapper = objectMapper;
        AtomicInteger threadCounter = new AtomicInteger(1);
        this.eventProcessorExecutor = Executors.newFixedThreadPool(NUM_PROCESSOR_THREADS, r -> {
            Thread t = new Thread(r, "kafka-event-processor-" + threadCounter.getAndIncrement());
            t.setDaemon(false);
            return t;
        });
    }

    @KafkaListener(topics = "${debezium.kafka.topic}", groupId = "debezium-cdc-listener-group")
    public void listen(ConsumerRecord<String, String> record) {
        try {
            if (record.value() == null) {
                return;
            }
            JsonNode root = objectMapper.readTree(record.value());
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
                    record.key(),
                    before,
                    after,
                    Instant.now()
            );

            eventProcessorExecutor.submit(() -> {
                try {
                    processor.processWithRetry(rowChange);
                } catch (Exception ex) {
                    log.error("Failed to process Kafka Debezium event for row: {}", record.key(), ex);
                }
            });
        } catch (Exception ex) {
            log.error("Failed to parse Kafka Debezium event", ex);
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
}
