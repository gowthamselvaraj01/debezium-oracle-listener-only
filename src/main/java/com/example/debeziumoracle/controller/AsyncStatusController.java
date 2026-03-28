package com.example.debeziumoracle.controller;

import com.example.debeziumoracle.service.AsyncEventProcessor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/async")
public class AsyncStatusController {

    private final AsyncEventProcessor asyncProcessor;

    public AsyncStatusController(AsyncEventProcessor asyncProcessor) {
        this.asyncProcessor = asyncProcessor;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "submitted", asyncProcessor.getSubmittedCount(),
                "completed", asyncProcessor.getCompletedCount(),
                "failed", asyncProcessor.getFailedCount(),
                "rejected", asyncProcessor.getRejectedCount(),
                "activeThreads", asyncProcessor.getActiveThreads(),
                "queueSize", asyncProcessor.getQueueSize(),
                "poolSize", asyncProcessor.getPoolSize()
        ));
    }
}
