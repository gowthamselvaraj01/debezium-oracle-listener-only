package com.example.debeziumoracle.service;

import com.example.debeziumoracle.model.RowChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class LoggingBusinessChangeProcessor implements BusinessChangeProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggingBusinessChangeProcessor.class);

    @Override
    public void process(RowChange rowChange) {
        log.info("Processing change source={} table={}.{} op={} rowId={} before={} after={}",
                rowChange.source(),
                rowChange.schemaName(),
                rowChange.tableName(),
                rowChange.operation(),
                rowChange.rowId(),
                rowChange.before(),
                rowChange.after());
    }
}
