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
package io.arenadata.kafka.clickhouse.writer.dao.impl;

import io.arenadata.kafka.clickhouse.writer.dao.ClickhouseDao;
import io.arenadata.kafka.clickhouse.writer.model.sql.ClickhouseInsertSqlRequest;
import io.arenadata.kafka.clickhouse.writer.model.sql.InsertSqlRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.List;

@Slf4j
public class ClickhouseDaoImpl implements ClickhouseDao {
    private final SQLClient sqlClient;

    public ClickhouseDaoImpl(Vertx vertx, DataSource clickhouseDataSource) {
        this.sqlClient = JDBCClient.create(vertx, clickhouseDataSource);
    }

    @Override
    public void insertRows(InsertSqlRequest request, Handler<AsyncResult<Integer>> handler) {
        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                List<JsonArray> params = ((ClickhouseInsertSqlRequest) request).getParams();
                conn.batchWithParams(request.getSql(), params, ar2 -> {
                    if (ar2.succeeded()) {
                        log.debug("Insert rows query executed successfully: [{}]", request.getSql());
                        List<Integer> result = ar2.result();
                        handler.handle(Future.succeededFuture(result.size()));
                    } else {
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("Connection error: " + ar1.cause().getMessage());
                handler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }
}
