# Debezium Oracle Listener Only

This project is the Debezium embedded Oracle listener extraction from `OracleDBListenerDemoIdea`.

## Included

- Spring Boot bootstrap
- Debezium embedded engine for Oracle
- JSON event parsing into a simple `RowChange` model
- Default logging-based change processor
- Local file-backed offset and schema history storage

## Configure

Update `src/main/resources/application.properties` with:

- `app.debezium.database-hostname`
- `app.debezium.database-port`
- `app.debezium.database-user`
- `app.debezium.database-password`
- `app.debezium.database-dbname`
- `app.debezium.database-pdb-name`
- `app.debezium.schema-include-list`
- `app.debezium.table-include-list`

## Run

```powershell
mvn spring-boot:run
```

Replace `LoggingBusinessChangeProcessor` if you want to route change events into your own business logic.
