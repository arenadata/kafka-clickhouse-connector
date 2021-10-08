# kafka-clickhouse-connector
A supporting service to the [Prostore main service](https://github.com/arenadata/prostore) `dtm-query-execution-core` that
communicates with the respective DBMS.

## Useful links
[Documentation (Rus)](https://arenadata.github.io/docs_prostore/getting_started/getting_started.html)

## Local deployment

### The cloning and building of kafka-clickhouse-connector
```shell script
#clone
git clone https://github.com/arenadata/kafka-clickhouse-connector
# build without any tests 
cd ~/kafka-clickhouse-connector
mvn clean
mvn install -DskipTests=true
```

### Connectors configuration
The connector configuration files `application.yml` are located in the respective folders
`kafka-clickhouse-writer/src/main/resources/` and `kafka-clickhouse-reader/src/main/resources/`.

To run connectors correctly one has to adjust respective configuration files for `kafka-clickhouse-writer` and `kafka-clickhouse-reader` to match the key values with the Prostore configuration, namely:
-    `env: name ~ env: name`,
-    `datasource: clickhouse: database ~ adqm: datasource: database`,
-    `datasource: clickhouse: user     ~ adqm: datasource: user`,
-    `datasource: clickhouse: password ~ adqm: datasource: password`,
-    `datasource: clickhouse: hosts    ~ adqm: datasource: hosts`.

The connector services look for the configuration in the same subfolders (target) where `kafka-clickhouse-writer-<version>.jar` and `kafka-clickhouse-reader-<version>.jar` are executed.
So we create the respective symbolic links
```shell script
sudo ln -s ~/kafka-clickhouse-connector/kafka-clickhouse-writer/src/main/resources/application.yml ~/kafka-clickhouse-connector/kafka-clickhouse-writer/target/application.yml
sudo ln -s ~/kafka-clickhouse-connector/kafka-clickhouse-reader/src/main/resources/application.yml ~/kafka-clickhouse-connector/kafka-clickhouse-reader/target/application.yml
```

### Run services
#### Run the kafka-clickhouse-writer connector as a single jar
```shell script
cd ~/kafka-clickhouse-connector/kafka-clickhouse-writer/target
java -Dspring.profiles.active=default -jar kafka-clickhouse-writer-<version>.jar
```
#### Run the kafka-clickhouse-reader connector as a single jar
```shell script
cd ~/kafka-clickhouse-connector/kafka-clickhouse-reader/target
java -Dspring.profiles.active=default -jar kafka-clickhouse-reader-<version>.jar
```

### Available endpoints
#### kafka-clickhouse-writer
```shell script
http://kafka-clickhouse-writer-host:8090/newdata/start [POST]
```

#### kafka-clickhouse-reader
```shell script
http://kafka-clickhouse-reader-host:8086/query [POST]
```
