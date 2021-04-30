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
package io.arenadata.kafka.clickhouse.writer.verticle;

import io.arenadata.kafka.clickhouse.writer.configuration.AppConfiguration;
import io.arenadata.kafka.clickhouse.writer.configuration.properties.VerticleProperties;
import io.arenadata.kafka.clickhouse.writer.controller.InsertDataController;
import io.arenadata.kafka.clickhouse.writer.controller.VersionController;
import io.arenadata.kafka.clickhouse.writer.model.DataTopic;
import io.arenadata.kafka.clickhouse.writer.verticle.handler.SendResponseHandler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class DataVerticle extends ConfigurableVerticle {
    private final SendResponseHandler sendResponseHandler;
    private final InsertDataController insertDataController;
    private final VersionController versionController;
    private final AppConfiguration configuration;
    private final VerticleProperties properties;
    private final Vertx vertxCore;

    @Override
    public void start() {
        Router router = Router.router(vertx);
        router.mountSubRouter("/", apiRouter());
        HttpServer httpServer = vertx.createHttpServer().requestHandler(router).listen(configuration.httpPort());
        log.info("server is running on port: [{}]", httpServer.actualPort());
        vertxCore.eventBus().consumer(DataTopic.SEND_RESPONSE.getValue(), this::handleSendResponse);
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        super.stop(stopFuture);
    }

    private void handleSendResponse(Message<String> contextId) {
        sendResponseHandler.handleSendResponse(contextId.body());
    }

    private Router apiRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().consumes("application/json");
        router.route().produces("application/json");
        router.post("/newdata/start").handler(insertDataController::startLoad);
        router.post("/newdata/stop").handler(insertDataController::stopLoad);
        router.get("/versions").handler(versionController::version);
        return router;
    }

    @Override
    public DeploymentOptions getDeploymentOptions() {
        val workerProps = properties.getNewDataWorker();
        return new DeploymentOptions().setWorker(false)
            .setWorkerPoolName(workerProps.getPoolName())
            .setWorkerPoolSize(workerProps.getPoolSize());
    }
}
