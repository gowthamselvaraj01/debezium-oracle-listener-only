package com.example.debeziumoracle.service;

import com.example.debeziumoracle.model.RowChange;

public interface BusinessChangeProcessor {

    void process(RowChange rowChange);
}
