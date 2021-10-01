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

import io.arenadata.kafka.clickhouse.reader.configuration.AppConfiguration;
import io.arenadata.kafka.clickhouse.reader.model.KafkaBrokerInfo;
import io.arenadata.kafka.clickhouse.reader.model.QueryRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@ExtendWith(VertxExtension.class)
class QueryControllerIT {

    @Autowired
    private WebClient webClient;

    @Autowired
    private AppConfiguration configuration;

    @Test
    void query(VertxTestContext testContext) throws Throwable {
        QueryRequest request = new QueryRequest("doc",
                "cost",
                "SELECT * FROM doc limit 1000",
                Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092)),
                "cost.doc.ex",
                1000,
                "",
                Collections.emptyList(),
                0,
                1);
        webClient.post(configuration.httpPort(), "localhost", "/query")
                .as(BodyCodec.jsonObject())
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
