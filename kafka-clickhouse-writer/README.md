# kafka-clickhouse-writer v3.3.0 
Supports part of functionality of MPP-W process (reading from kafka and writing data into Clickhouse database). 

## How to build

```shell script
mvn clean package
```

## How to run
Default configuration can be found in the project src/main/resources/application.yml
It requires to provide to Clickhouse datasource. Kafka configuration can be left as is. 

```shell script
java -jar target/kafka-clickhouse-writer-<version>.jar -Dspring.profiles.active=dev -Dspring.config.location=<path ti config>/application.yml
# or
mvn spring-boot:run
```

After starting connector, it will accept incoming request with data load by the next url
```shell script
http://kafka-clickhouse-writer-host:8090/newdata/start
```

This will allow to accept incoming MPP-W request from DTM core and write data into clickhouse.