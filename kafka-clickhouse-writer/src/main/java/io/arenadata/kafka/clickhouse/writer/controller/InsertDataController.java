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
package io.arenadata.kafka.clickhouse.writer.controller;

import io.arenadata.kafka.clickhouse.writer.model.InsertDataContext;
import io.arenadata.kafka.clickhouse.writer.model.InsertDataRequest;
import io.arenadata.kafka.clickhouse.writer.model.StopRequest;
import io.arenadata.kafka.clickhouse.writer.repository.InsertDataContextRepository;
import io.arenadata.kafka.clickhouse.writer.service.executor.InsertDataRequestExecutor;
import io.arenadata.kafka.clickhouse.writer.model.DataTopic;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

@Slf4j
@Component
@RequiredArgsConstructor
public class InsertDataController {
    private final InsertDataContextRepository dataContextRepository;
    private final InsertDataRequestExecutor executor;
    private final Vertx vertx;

    public void startLoad(RoutingContext context) {
        executor.execute(getOrCreate(context));
    }

    private InsertDataContext getOrCreate(RoutingContext ctx) {
        InsertDataContext context;
        try {
            InsertDataRequest request;
            request = ctx.getBodyAsJson().mapTo(InsertDataRequest.class);
            log.info("Received for path: [{}] data write request: [{}]", ctx.normalisedPath(), request);
            context = dataContextRepository.get(InsertDataContext.createContextId(request.getKafkaTopic(), request.getRequestId()))
                .orElseGet(() -> new InsertDataContext(request, ctx));
            context.setContext(ctx);
        } catch (Exception e) {
            log.error("Error while creating context from {}, url {}", ctx.getBodyAsString(), ctx.request().absoluteURI());
            ctx.fail(e);
            throw e;
        }
        return context;
    }

    public void stopLoad(RoutingContext ctx) {
        try {
            StopRequest request;
            request = ctx.getBodyAsJson().mapTo(StopRequest.class);
            log.info("Received for path: [{}] data write request: [{}]", ctx.normalisedPath(), request);
            String contextId = InsertDataContext.createContextId(request.getKafkaTopic(), request.getRequestId());
            dataContextRepository.get(contextId)
                .ifPresent(insertDataContext -> vertx.eventBus().send(DataTopic.SEND_RESPONSE.getValue(), contextId));
            ctx.response()
                .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                .setStatusCode(200)
                .end("");
        } catch (Exception e) {
            log.error("Error while creating context from {}, url {}", ctx.getBodyAsString(), ctx.request().absoluteURI());
            ctx.fail(e);
            throw e;
        }
    }

}
