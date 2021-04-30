/*
 * Copyright © 2021 Kafka Clickhouse Reader
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.kafka.clickhouse.reader.configuration;

import io.arenadata.kafka.clickhouse.reader.configuration.properties.ClickHouseProperties;
import io.arenadata.kafka.clickhouse.reader.converter.ClickhouseTypeToSqlTypeConverter;
import io.arenadata.kafka.clickhouse.reader.converter.transformer.ColumnTransformer;
import io.arenadata.kafka.clickhouse.reader.model.ColumnType;
import io.arenadata.kafka.clickhouse.reader.service.DatabaseExecutor;
import io.arenadata.kafka.clickhouse.reader.service.impl.ClickhouseDatabaseExecutor;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class QueryConfiguration {

    @Bean("clickhouseDatabaseExecutorList")
    public List<DatabaseExecutor> clickhouseExecutors(Vertx vertx,
                                                      ClickHouseProperties clickhouseProperties,
                                                      @Qualifier("clickhouseTransformerMap") Map<ColumnType, Map<Class<?>, ColumnTransformer>> columnTypeMap) {
        final List<String> hostList = getHostList(clickhouseProperties.getHosts());
        final List<DatabaseExecutor> databaseExecutors = hostList.stream()
                .map(host -> {
                    String url = String.format("jdbc:clickhouse://%s/%s", host.trim(),
                            clickhouseProperties.getDatabase());
                    Properties props = new Properties();
                    props.put("user", clickhouseProperties.getUser());
                    props.put("password", clickhouseProperties.getPassword());
                    return new ClickhouseDatabaseExecutor(vertx,
                            new ClickhouseDataSource(url, props),
                            new ClickhouseTypeToSqlTypeConverter(columnTypeMap));
                })
                .collect(Collectors.toList());
        log.debug("Created clickhouse executors {}", hostList);
        return databaseExecutors;
    }

    private List<String> getHostList(String hosts) {
        return Arrays.stream(hosts.trim().split(",")).collect(Collectors.toList());
    }

}
