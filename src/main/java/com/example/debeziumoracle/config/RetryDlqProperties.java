package com.example.debeziumoracle.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.retry-dlq")
public class RetryDlqProperties {

    private int maxRetries = 3;
    private long initialBackoffMs = 1000;
    private double backoffMultiplier = 2.0;
    private long maxBackoffMs = 10000;
    private String dlqFilePath = "data/dlq";
    private int dlqMaxSize = 10000;

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public void setInitialBackoffMs(long initialBackoffMs) {
        this.initialBackoffMs = initialBackoffMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public void setMaxBackoffMs(long maxBackoffMs) {
        this.maxBackoffMs = maxBackoffMs;
    }

    public String getDlqFilePath() {
        return dlqFilePath;
    }

    public void setDlqFilePath(String dlqFilePath) {
        this.dlqFilePath = dlqFilePath;
    }

    public int getDlqMaxSize() {
        return dlqMaxSize;
    }

    public void setDlqMaxSize(int dlqMaxSize) {
        this.dlqMaxSize = dlqMaxSize;
    }
}
