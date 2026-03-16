package com.example.debeziumoracle.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.debezium")
public class DebeziumCaptureProperties {

    private String connectorName = "oracle-embedded-connector";
    private String databaseHostname;
    private int databasePort = 1521;
    private String databaseUser;
    private String databasePassword;
    private String databaseDbname;
    private String databasePdbName;
    private String tableIncludeList;
    private String schemaIncludeList;
    private String topicPrefix = "oracle-cdc";
    private String offsetFileName = "data/debezium-offsets.dat";
    private String schemaHistoryFileName = "data/debezium-schema-history.dat";

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    public String getDatabaseHostname() {
        return databaseHostname;
    }

    public void setDatabaseHostname(String databaseHostname) {
        this.databaseHostname = databaseHostname;
    }

    public int getDatabasePort() {
        return databasePort;
    }

    public void setDatabasePort(int databasePort) {
        this.databasePort = databasePort;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public void setDatabaseUser(String databaseUser) {
        this.databaseUser = databaseUser;
    }

    public String getDatabasePassword() {
        return databasePassword;
    }

    public void setDatabasePassword(String databasePassword) {
        this.databasePassword = databasePassword;
    }

    public String getDatabaseDbname() {
        return databaseDbname;
    }

    public void setDatabaseDbname(String databaseDbname) {
        this.databaseDbname = databaseDbname;
    }

    public String getDatabasePdbName() {
        return databasePdbName;
    }

    public void setDatabasePdbName(String databasePdbName) {
        this.databasePdbName = databasePdbName;
    }

    public String getTableIncludeList() {
        return tableIncludeList;
    }

    public void setTableIncludeList(String tableIncludeList) {
        this.tableIncludeList = tableIncludeList;
    }

    public String getSchemaIncludeList() {
        return schemaIncludeList;
    }

    public void setSchemaIncludeList(String schemaIncludeList) {
        this.schemaIncludeList = schemaIncludeList;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getOffsetFileName() {
        return offsetFileName;
    }

    public void setOffsetFileName(String offsetFileName) {
        this.offsetFileName = offsetFileName;
    }

    public String getSchemaHistoryFileName() {
        return schemaHistoryFileName;
    }

    public void setSchemaHistoryFileName(String schemaHistoryFileName) {
        this.schemaHistoryFileName = schemaHistoryFileName;
    }
}
