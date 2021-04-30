/*
 * Copyright © 2021 Kafka Clickhouse Writer
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
package io.arenadata.kafka.clickhouse.writer.configuration;

import io.arenadata.kafka.clickhouse.writer.configuration.properties.ClickHouseProperties;
import io.arenadata.kafka.clickhouse.writer.configuration.properties.EnvProperties;
import io.arenadata.kafka.clickhouse.writer.service.executor.impl.ClickhouseExecutor;
import io.arenadata.kafka.clickhouse.writer.dao.impl.ClickhouseDaoImpl;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class QueryConfiguration {

    @Bean("clickhouseDaoImpl")
    public ClickhouseDaoImpl clickhouse(Vertx vertx, ClickHouseProperties clickhouseProperties, EnvProperties envProperties) {
        String url = String.format("jdbc:clickhouse://%s/%s", clickhouseProperties.getHosts(),
            clickhouseProperties.getDatabase());
        Properties props = new Properties();
        props.put("user", clickhouseProperties.getUser());
        props.put("password", clickhouseProperties.getPassword());
        DataSource dataSource = new ClickhouseDataSource(url, props);
        return new ClickhouseDaoImpl(vertx, dataSource);
    }

    @Bean
    public List<ClickhouseExecutor> clickhouseExecutors(Vertx vertx, ClickHouseProperties clickhouseProperties) {
        return Arrays.stream(clickhouseProperties.getHosts().split(","))
            .map(host -> {
                String url = String.format("jdbc:clickhouse://%s/%s", host,
                    clickhouseProperties.getDatabase());
                Properties props = new Properties();
                props.put("user", clickhouseProperties.getUser());
                props.put("password", clickhouseProperties.getPassword());
                DataSource dataSource = new ClickhouseDataSource(url, props);
                return new ClickhouseExecutor(new ClickhouseDaoImpl(vertx, dataSource));
            })
            .collect(Collectors.toList());
    }
}
