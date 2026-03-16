package com.example.debeziumoracle;

import com.example.debeziumoracle.config.DebeziumCaptureProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(DebeziumCaptureProperties.class)
public class DebeziumOracleListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DebeziumOracleListenerApplication.class, args);
    }
}
