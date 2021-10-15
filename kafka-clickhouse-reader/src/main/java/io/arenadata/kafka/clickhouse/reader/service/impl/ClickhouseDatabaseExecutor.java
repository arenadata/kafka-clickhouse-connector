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
package io.arenadata.kafka.clickhouse.reader.service.impl;

import io.arenadata.kafka.clickhouse.reader.converter.SqlTypeConverter;
import io.arenadata.kafka.clickhouse.reader.model.QueryRequest;
import io.arenadata.kafka.clickhouse.reader.model.QueryResultItem;
import io.arenadata.kafka.clickhouse.reader.service.DatabaseExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLRowStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ClickhouseDatabaseExecutor implements DatabaseExecutor {

    private final SQLClient sqlClient;
    private final SqlTypeConverter typeConverter;

    public ClickhouseDatabaseExecutor(Vertx vertx, DataSource clickhouseDataSource, SqlTypeConverter typeConverter) {
        this.sqlClient = JDBCClient.create(vertx, clickhouseDataSource);
        this.typeConverter = typeConverter;
    }

    @Override
    public void execute(QueryRequest query,
                        Handler<AsyncResult<QueryResultItem>> itemHandler,
                        Handler<AsyncResult<Void>> handler) {
        sqlClient.getConnection(conn -> {
            if (conn.succeeded()) {
                val columnTypes = getColumnTypes(query.getAvroSchema());
                val sqlConn = conn.result();
                val chunkNumber = new AtomicInteger(1);
                val dataSet = new ArrayList<List<?>>();
                log.debug("Start execution query [{}]", query.getSql());
                sqlConn.queryStream(query.getSql(), ar -> {
                    if (ar.succeeded()) {
                        final SQLRowStream sqlRowStream = ar.result();
                        sqlRowStream.handler(row -> {
                            val rowData = new ArrayList<>();
                            for (int i = 0; i < row.size(); i++) {
                                try {
                                    rowData.add(typeConverter.convert(columnTypes.get(i), row.getValue(i)));
                                } catch (Exception e) {
                                    final String errMsg = String.format("Error in converting row column [%d] value [%s]",
                                            i,
                                            row.getValue(i));
                                    log.error(errMsg, e);
                                    sqlRowStream.close();
                                    sqlConn.close();
                                    handler.handle(Future.failedFuture(errMsg));
                                }
                            }
                            dataSet.add(rowData);
                            if (dataSet.size() == query.getChunkSize()) {
                                itemHandler.handle(Future.succeededFuture(
                                        new QueryResultItem(query.getKafkaTopic(),
                                                query.getTable(),
                                                new ArrayList<>(dataSet),
                                                chunkNumber.getAndIncrement(),
                                                false))
                                );
                                dataSet.clear();
                            }
                        }).endHandler(v -> {
                            itemHandler.handle(Future.succeededFuture(
                                    new QueryResultItem(query.getKafkaTopic(),
                                            query.getTable(),
                                            new ArrayList<>(dataSet),
                                            chunkNumber.getAndIncrement(),
                                            true))
                            );
                            dataSet.clear();
                            sqlConn.close();
                            log.debug("Stop execution query [{}]", query.getSql());
                            handler.handle(Future.succeededFuture());
                        }).exceptionHandler(er -> {
                            log.error("Error in row stream", er.getCause());
                        });
                    } else {
                        log.error("Error executing query [{}]", query.getSql(), ar.cause());
                        sqlConn.close();
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
            } else {
                log.error("Error creating connection by request [{}]",
                        query);
                handler.handle(Future.failedFuture(conn.cause()));
            }
        });
    }

    private List<Schema.Type> getColumnTypes(String schemaString) {
        val schema = new Schema.Parser().parse(schemaString);
        val fields = schema.getFields();
        List<Schema.Type> result = new ArrayList<>();
        for (val field : fields) {
            val typeSchema = field.schema();
            if (typeSchema.isUnion()) {
                val first = typeSchema.getTypes().stream()
                        .filter(schema2 -> schema2.getType() != Schema.Type.NULL)
                        .findFirst();
                if (first.isPresent()) {
                    result.add(first.get().getType());
                } else {
                    result.add(Schema.Type.NULL);
                }
                continue;
            }

            result.add(typeSchema.getType());
        }
        return result;
    }
}
