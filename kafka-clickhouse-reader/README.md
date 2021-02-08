# kafka-clickhouse-reader v3.3.0
Supports part of functionality of MPP-R process (reading data from clickhouse and writing into kafka).

## How to build

```shell script
mvn clean package
```

## How to run
Default configuration can be found in the project src/main/resources/application.yml

```shell script
java -jar target/kafka-clickhouse-reader-<version>.jar -Dspring.profiles.active=dev -Dspring.config.location=<path to config>/application.yml
# or
mvn spring-boot:run
```

After starting connector, it will accept incoming request with data load by the next url
```shell script
http://kafka-clickhouse-reader-host:8086/query
```

This will allow to accept incoming MPP-R request from DTM core and write data from clickhouse into kafka.