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
package io.arenadata.kafka.clickhouse.reader.controller;

import io.arenadata.kafka.clickhouse.reader.model.QueryRequest;
import io.arenadata.kafka.clickhouse.reader.service.QueryDispatcher;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Slf4j
@Component
public class QueryController {

    private final QueryDispatcher queryDispatcher;

    @Autowired
    public QueryController(QueryDispatcher queryDispatcher) {
        this.queryDispatcher = queryDispatcher;
    }

    public void query(RoutingContext context) {
        QueryRequest query;
        try {
            String bodyAsString = context.getBodyAsString();
            log.info("Received request {}", bodyAsString);
            query = Json.decodeValue(bodyAsString, QueryRequest.class);
            log.info("Received request sql=[{}], chunkSize=[{}]", query.getSql(), query.getChunkSize());
        } catch (DecodeException e) {
            log.error("Decode request error", e);
            context.fail(e);
            return;
        }
        queryDispatcher.dispatch(query)
                .onComplete(ar -> {
                    if (ar.failed()) {
                        log.error("Execute request error", ar.cause());
                        context.fail(ar.cause());
                    } else {
                        context.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON_VALUE)
                                .setStatusCode(OK.code())
                                .end();
                    }
                });
    }

}
