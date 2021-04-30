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

import io.arenadata.kafka.clickhouse.reader.converter.impl.*;
import io.arenadata.kafka.clickhouse.reader.converter.transformer.ColumnTransformer;
import io.arenadata.kafka.clickhouse.reader.model.ColumnType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static io.arenadata.kafka.clickhouse.reader.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSS[SSS]]";

    @Bean("clickhouseTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> clickhouseTransformerMap(@Value("${timezone}") String timeZone) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>(getCommonTransformerMap());
        transformerMap.put(ColumnType.DATE, getTransformerMap(new IntegerFromLocalDateNumberTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new LongFromNumberTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(new LongFromLocalDateTimeStringTransformer(
                        DateTimeFormatter.ofPattern(DATE_TIME_FORMAT),
                        getTimeZone(timeZone)),
                new LongTsFromLongTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer(),
                new BooleanFromNumericTransformer()));
        return transformerMap;
    }

    private Map<ColumnType, Map<Class<?>, ColumnTransformer>> getCommonTransformerMap() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new LongFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new FloatFromNumberTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, transformerMap.get(ColumnType.VARCHAR));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new BigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new DoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.UUID, getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }

    private ZoneId getTimeZone(String timeZone) {
        return ZoneId.of(timeZone);
    }
}
