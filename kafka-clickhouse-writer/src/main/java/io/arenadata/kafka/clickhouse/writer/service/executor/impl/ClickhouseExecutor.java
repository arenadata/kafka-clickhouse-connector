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
package io.arenadata.kafka.clickhouse.writer.service.executor.impl;

import io.arenadata.kafka.clickhouse.writer.model.InsertDataContext;
import io.arenadata.kafka.clickhouse.writer.model.kafka.InsertChunk;
import io.arenadata.kafka.clickhouse.writer.dao.ClickhouseDao;
import io.arenadata.kafka.clickhouse.writer.service.executor.DataSourceExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;

@Slf4j
public class ClickhouseExecutor implements DataSourceExecutor {
    private final ClickhouseDao clickhouseDao;

    public ClickhouseExecutor(ClickhouseDao clickhouseDao) {
        this.clickhouseDao = clickhouseDao;
    }

    @Override
    public void processChunk(InsertDataContext context, InsertChunk chunk, Handler<AsyncResult<Integer>> handler) {
        insertRows(chunk)
            .onSuccess(ar -> handler.handle(Future.succeededFuture(ar)))
            .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
    }

    private Future<Integer> insertRows(InsertChunk chunk) {
        return Future.future((Promise<Integer> promise) -> {
            List<JsonArray> rows = chunk.getInsertSqlRequest().getParams();
            val insertedRowsCount = rows.size();
            log.info("Sent lines to write: [{}]", insertedRowsCount);
            clickhouseDao.insertRows(chunk.getInsertSqlRequest(), ar -> {
                if (ar.succeeded()) {
                    log.info("Sent lines to write: [{}] - COMPLETED", insertedRowsCount);
                    promise.complete(insertedRowsCount);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }
}
