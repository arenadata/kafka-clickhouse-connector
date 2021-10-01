/*
 * Copyright © 2021 Arenadata Software LLC
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

import io.arenadata.kafka.clickhouse.writer.configuration.properties.DatamartProperties;
import io.arenadata.kafka.clickhouse.writer.configuration.properties.EnvProperties;
import io.arenadata.kafka.clickhouse.writer.model.InsertDataContext;
import io.arenadata.kafka.clickhouse.writer.model.InsertDataRequest;
import io.arenadata.kafka.clickhouse.writer.model.sql.ClickhouseInsertSqlRequest;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;
import lombok.val;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InsertSqlRequestFactoryImplTest {
    public static final int EXPECTED_DELTA = 10;
    private static final String EXPECTED_SQL = "insert into local__test_datamart.test_table_ext_shard (id,name) values (?,?)";
    private InsertRequestFactoryImpl factory;
    private InsertDataContext context;
    private final RoutingContext routingContext = mock(RoutingContext.class);

    @BeforeEach
    public void before() {
        EnvProperties envProperties = new EnvProperties();
        envProperties.setName("local");
        factory = new InsertRequestFactoryImpl(new DatamartProperties("sys_from"), envProperties);

        HttpServerRequest httpServerRequest = mock(HttpServerRequest.class);
        when(routingContext.request()).thenReturn(httpServerRequest);
        initContext();
    }

    private void initContext() {
        val request = new InsertDataRequest();
        request.setDatamart("test_datamart");
        request.setKafkaTopic("kafka_topic");
        request.setHotDelta(EXPECTED_DELTA);
        request.setTableName("test_table");
        request.setSchema(SchemaBuilder.record("test").fields()
                .optionalInt("id")
                .name("name")
                .type()
                .nullable()
                .stringBuilder()
                .prop("avro.java.string", "String")
                .endString()
                .noDefault()
                .endRecord());
        context = new InsertDataContext(request, routingContext);
        context.setKeyColumns(Arrays.asList("id", "name"));
    }

    @Test
    void createInsertRequestWithoutHotDelta() {
        context.setInsertSql(factory.getSql(context));
        ClickhouseInsertSqlRequest request = factory.create(context, getRowsWithoutDelta());
        assertInsertRequest(request, 2);
    }

    private List<GenericRecord> getRowsWithoutDelta() {
        return IntStream.range(0, 10)
                .mapToObj(it -> getRowWithoutDelta(it, "name_" + it))
                .collect(Collectors.toList());
    }

    private GenericData.Record getRowWithoutDelta(int expectedId, String expectedName) {
        val schema = SchemaBuilder.record("test").fields()
                .optionalString("id")
                .optionalString("name")
                .endRecord();
        return new GenericRecordBuilder(schema)
                .set("id", expectedId)
                .set("name", expectedName)
                .build();
    }

    private void assertInsertRequest(ClickhouseInsertSqlRequest actual, Integer expectedTuples) {
        assertEquals(EXPECTED_SQL, actual.getSql());
        assertEquals(10, actual.getParams().size());
        JsonArray tuple = actual.getParams().get(0);
        assertEquals(expectedTuples, tuple.size());
    }

    @Test
    void createInsertRequestWithHotDelta() {
        context.setInsertSql(factory.getSql(context));
        ClickhouseInsertSqlRequest request = factory.create(context, getRowsWithDelta());
        assertInsertRequest(request, 3);
    }

    private List<GenericRecord> getRowsWithDelta() {
        return IntStream.range(0, 10)
                .mapToObj(it -> getRowWithDelta(it, "name_" + it))
                .collect(Collectors.toList());
    }

    private GenericData.Record getRowWithDelta(int expectedId, String expectedName) {
        val schema = SchemaBuilder.record("test").fields()
                .optionalString("id")
                .optionalString("name")
                .optionalString(InsertRequestFactoryImpl.DEFAULT_HOT_DELTA_FIELD_NAME)
                .endRecord();
        return new GenericRecordBuilder(schema)
                .set("id", expectedId)
                .set("name", expectedName)
                .set(InsertRequestFactoryImpl.DEFAULT_HOT_DELTA_FIELD_NAME, 5)
                .build();
    }
}
