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
package io.arenadata.kafka.clickhouse.writer.factory.impl;

import com.google.common.base.Strings;
import io.arenadata.kafka.clickhouse.writer.configuration.properties.DatamartProperties;
import io.arenadata.kafka.clickhouse.writer.configuration.properties.EnvProperties;
import io.arenadata.kafka.clickhouse.writer.factory.InsertRequestFactory;
import io.arenadata.kafka.clickhouse.writer.model.InsertDataContext;
import io.arenadata.kafka.clickhouse.writer.model.sql.ClickhouseInsertSqlRequest;
import io.vertx.core.json.JsonArray;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
public class InsertRequestFactoryImpl implements InsertRequestFactory {
    public static final String DEFAULT_HOT_DELTA_FIELD_NAME = "sys_from";
    private static final String TEMPLATE_SQL = "insert into %s.%s (%s) values (%s)";
    private final String deltaHotFieldName;
    private final EnvProperties envProperties;

    @Autowired
    public InsertRequestFactoryImpl(DatamartProperties properties, EnvProperties envProperties) {
        deltaHotFieldName = Strings.isNullOrEmpty(properties.getHotDeltaFieldName()) ?
                DEFAULT_HOT_DELTA_FIELD_NAME : properties.getHotDeltaFieldName();
        this.envProperties = envProperties;
    }

    @Override
    public ClickhouseInsertSqlRequest create(InsertDataContext context, List<GenericRecord> rows) {
        if (!rows.isEmpty()) {
            return createSqlRequest(context, rows);
        } else return new ClickhouseInsertSqlRequest(context.getInsertSql(), new ArrayList<>());
    }

    private ClickhouseInsertSqlRequest createSqlRequest(InsertDataContext context, List<GenericRecord> rows) {
        val fields = rows.get(0).getSchema().getFields();
        val hotDeltaPair = getIndexedHotDeltaPair(context, fields);
        val batch = getParams(rows, fields, hotDeltaPair);
        return new ClickhouseInsertSqlRequest(context.getInsertSql(), batch);
    }

    private List<JsonArray> getParams(List<GenericRecord> rows, List<Schema.Field> fields, Pair<Integer, Integer> hotDeltaPair) {
        return rows.stream()
                .map(row -> getJsonParam(fields, row, null))
                .collect(Collectors.toList());
    }

    private JsonArray getJsonParam(List<Schema.Field> fields, GenericRecord row, Pair<Integer, Integer> hotDeltaPair) {
        val tuple = new JsonArray();
        if (hotDeltaPair != null && hotDeltaPair.getKey() != -1) {
            IntStream.range(0, fields.size()).mapToObj(it -> replaceHotDelta(it, row, hotDeltaPair)).forEach(f -> {
                tuple.add(convertValueIfNeeded(f));
            });
        } else {
            IntStream.range(0, fields.size()).mapToObj(row::get).forEach(f -> {
                tuple.add(convertValueIfNeeded(f));
            });
            if (hotDeltaPair != null) {
                tuple.add(hotDeltaPair.getValue());
            }
        }
        return tuple;
    }

    private Object convertValueIfNeeded(Object value) {
        if (value instanceof LocalDate) {
            return ((LocalDate) value).toEpochDay();
        } else if (value instanceof LocalTime) {
            return ((LocalTime) value).toNanoOfDay();
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000;
        }
        return value;
    }

    private Pair<Integer, Integer> getIndexedHotDeltaPair(InsertDataContext context, List<Schema.Field> fields) {
        val hotDeltaField = fields.stream().filter(field -> deltaHotFieldName
                .equalsIgnoreCase(field.name()))
                .findAny()
                .orElse(null);
        return Pair.of(hotDeltaField == null ? -1 : hotDeltaField.pos(), context.getRequest().getHotDelta());
    }

    private Object replaceHotDelta(int pos, GenericRecord row, Pair<Integer, Integer> hotDeltaPair) {
        return pos == hotDeltaPair.getKey() ? hotDeltaPair.getValue() : row.get(pos);
    }

    @Override
    public String getSql(InsertDataContext context) {
        val fields = context.getRequest().getSchema().getFields();
        val hotDeltaPair = getIndexedHotDeltaPair(context, fields);
        return getInsertSql(context, fields, hotDeltaPair.getKey());
    }

    private String getInsertSql(InsertDataContext context, List<Schema.Field> fields, Integer hotDeltaPos) {
        val insertColumns = fields.stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        val insertValues = IntStream.range(1, fields.size() + 1)
                .mapToObj(pos -> "?")
                .collect(Collectors.toList());
        val request = context.getRequest();
        return String.format(TEMPLATE_SQL,
                envProperties.getName() + envProperties.getDelimiter() + request.getDatamart(),
                request.getTableName() + "_ext_shard", String.join(",", insertColumns),
                String.join(",", insertValues));
    }

    private void tryAddHotDelta(List<String> insertColumns,
                                List<String> insertValues,
                                Integer hotDeltaPos) {
        if (hotDeltaPos == -1) {
            insertColumns.add(deltaHotFieldName);
            int hotDeltaValueIndex = insertValues.size() + 1;
            insertValues.add("$" + hotDeltaValueIndex);
        }
    }
}
