package com.example.debeziumoracle.controller;

import com.example.debeziumoracle.model.FailedEvent;
import com.example.debeziumoracle.service.DeadLetterQueueService;
import com.example.debeziumoracle.service.RetryableChangeProcessor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dlq")
public class DlqController {

    private final DeadLetterQueueService dlqService;
    private final RetryableChangeProcessor retryableProcessor;

    public DlqController(DeadLetterQueueService dlqService,
                         RetryableChangeProcessor retryableProcessor) {
        this.dlqService = dlqService;
        this.retryableProcessor = retryableProcessor;
    }

    @GetMapping
    public ResponseEntity<List<FailedEvent>> listAll() {
        return ResponseEntity.ok(dlqService.getAll());
    }

    @GetMapping("/count")
    public ResponseEntity<Map<String, Integer>> count() {
        return ResponseEntity.ok(Map.of("count", dlqService.size()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<FailedEvent> getById(@PathVariable String id) {
        return dlqService.getById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<Map<String, String>> replayOne(@PathVariable String id) {
        try {
            retryableProcessor.replayFromDlq(id);
            return ResponseEntity.ok(Map.of("status", "replayed", "id", id));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/replay-all")
    public ResponseEntity<Map<String, Object>> replayAll() {
        int replayed = retryableProcessor.replayAll();
        return ResponseEntity.ok(Map.of(
                "status", "completed",
                "replayed", replayed,
                "remaining", dlqService.size()
        ));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, String>> deleteOne(@PathVariable String id) {
        boolean removed = dlqService.remove(id);
        if (removed) {
            return ResponseEntity.ok(Map.of("status", "deleted", "id", id));
        }
        return ResponseEntity.notFound().build();
    }

    @DeleteMapping
    public ResponseEntity<Map<String, Object>> deleteAll() {
        int cleared = dlqService.clear();
        return ResponseEntity.ok(Map.of("status", "cleared", "count", cleared));
    }
}
