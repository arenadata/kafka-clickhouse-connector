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
package io.arenadata.kafka.clickhouse.reader.upstream;

import io.arenadata.kafka.clickhouse.reader.avro.codec.AvroQueryResultEncoder;
import io.arenadata.kafka.clickhouse.reader.avro.model.AvroQueryResultRow;
import io.arenadata.kafka.clickhouse.reader.model.DtmQueryResponseMetadata;
import io.arenadata.kafka.clickhouse.reader.model.QueryRequest;
import io.arenadata.kafka.clickhouse.reader.model.QueryResultItem;
import io.arenadata.kafka.clickhouse.reader.service.PublishService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.util.stream.Collectors;

@Slf4j
public class QueryResultUpstream implements Upstream<QueryResultItem> {

    private final PublishService publishService;
    private final Schema schema;
    private final AvroQueryResultEncoder resultEncoder;

    public QueryResultUpstream(PublishService publishService, Schema schema) {
        this.publishService = publishService;
        this.schema = schema;
        this.resultEncoder = new AvroQueryResultEncoder();
    }

    @Override
    public void push(QueryRequest queryRequest, QueryResultItem item, Handler<AsyncResult<Void>> handler) {
        try {
            final byte[] bytes = resultEncoder.encode(item.getDataSet().stream()
                    .map(row -> new AvroQueryResultRow(schema, row))
                    .collect(Collectors.toList()), schema);
            send(queryRequest, item, bytes, handler);
        } catch (Exception e) {
            log.error("Error in sending message to kafka topic: table [{}] chunkNumber [{}] topic [{}]",
                    item.getTable(),
                    item.getChunkNumber(),
                    item.getKafkaTopic());
            handler.handle(Future.failedFuture(e));
        }
    }

    private void send(QueryRequest queryRequest, QueryResultItem item, byte[] compressedBytes, Handler<AsyncResult<Void>> handler) {
        DtmQueryResponseMetadata response = new DtmQueryResponseMetadata(item.getTable(),
                queryRequest.getStreamNumber(),
                queryRequest.getStreamTotal(),
                item.getChunkNumber(),
                item.getIsLastChunk());
        this.publishService.publishQueryResult(queryRequest.getKafkaBrokers(),
                item.getKafkaTopic(),
                response,
                compressedBytes,
                handler);
    }

    @Override
    public void close() {
    }

}
