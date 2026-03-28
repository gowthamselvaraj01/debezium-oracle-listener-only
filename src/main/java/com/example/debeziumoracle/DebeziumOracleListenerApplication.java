package com.example.debeziumoracle;

import com.example.debeziumoracle.config.AsyncProcessingProperties;
import com.example.debeziumoracle.config.DebeziumCaptureProperties;
import com.example.debeziumoracle.config.RetryDlqProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({DebeziumCaptureProperties.class, RetryDlqProperties.class, AsyncProcessingProperties.class})
public class DebeziumOracleListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DebeziumOracleListenerApplication.class, args);
    }
}
