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

import io.arenadata.kafka.clickhouse.reader.factory.UpstreamFactory;
import io.arenadata.kafka.clickhouse.reader.model.QueryRequest;
import io.arenadata.kafka.clickhouse.reader.model.QueryResultItem;
import io.arenadata.kafka.clickhouse.reader.service.DatabaseExecutor;
import io.arenadata.kafka.clickhouse.reader.service.QueryExecutorService;
import io.arenadata.kafka.clickhouse.reader.upstream.Upstream;
import io.arenadata.kafka.clickhouse.reader.verticle.TaskVerticleExecutor;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
public abstract class BaseQueryExecutorService implements QueryExecutorService {

    private final List<DatabaseExecutor> databaseExecutors;
    private final UpstreamFactory<QueryResultItem> upstreamFactory;
    private final TaskVerticleExecutor taskExecutor;

    public BaseQueryExecutorService(List<DatabaseExecutor> databaseExecutors,
                                    UpstreamFactory<QueryResultItem> upstreamFactory,
                                    TaskVerticleExecutor taskExecutor) {
        this.databaseExecutors = databaseExecutors;
        this.upstreamFactory = upstreamFactory;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void execute(QueryRequest query, Handler<AsyncResult<Void>> handler) {
        executeInternal(query, handler);
    }

    private void executeInternal(QueryRequest query, Handler<AsyncResult<Void>> handler) {
        List<Future> queryFutures = new ArrayList<>();
        List<Future> messageFutures = new ArrayList<>();
        IntStream.range(0, databaseExecutors.size()).forEach(i -> {
            final QueryRequest queryRequest = query.copy();
            queryRequest.setStreamTotal(databaseExecutors.size());
            queryRequest.setStreamNumber(i);
            queryFutures.add(Future.future((Promise<Void> promise) -> {
                taskExecutor.execute(executeQuery(databaseExecutors.get(i), queryRequest, messageFutures), promise);
            }));
        });
        CompositeFuture.join(queryFutures)
                .onSuccess(success -> {
                    CompositeFuture.join(messageFutures)
                            .onSuccess(msgSuccess -> {
                                log.debug("Query executed successfully {}", query);
                                handler.handle(Future.succeededFuture());
                            })
                            .onFailure(fail -> {
                                log.error("Error in processing message into kafka topic by query {}", query, fail);
                                handler.handle(Future.failedFuture(fail));
                            });
                })
                .onFailure(fail -> {
                    log.error("Error in executing query {}", query, fail);
                    handler.handle(Future.failedFuture(fail));
                });
    }

    private Handler<Promise<Void>> executeQuery(DatabaseExecutor executor,
                                                QueryRequest query,
                                                List<Future> messageFutures) {
        val upstream = upstreamFactory.create(query.getAvroSchema());
        return ((Promise<Void> p) -> {
            executor.execute(query, ir -> {
                if (ir.succeeded()) {
                    final QueryResultItem result = ir.result();
                    messageFutures.add(pushMessage(query, upstream, result));
                } else {
                    p.fail(ir.cause());
                }
            }, p);
        });
    }


    private Future<Void> pushMessage(QueryRequest query, Upstream<QueryResultItem> upstream,
                                     QueryResultItem resultItem) {
        return Future.future(promise ->
                upstream.push(query, resultItem, ur -> {
                    if (ur.succeeded()) {
                        log.debug("Chunk [{}] for table [{}] was pushed successfully into topic [{}] isLast [{}]",
                                resultItem.getChunkNumber(),
                                resultItem.getTable(),
                                resultItem.getKafkaTopic(),
                                resultItem.getIsLastChunk());
                        promise.complete();
                    } else {
                        log.error("Error sending message to kafka by query {}", query, ur.cause());
                        promise.fail(ur.cause());
                    }
                }));
    }
}
