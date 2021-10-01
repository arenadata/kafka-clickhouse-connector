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
package io.arenadata.kafka.clickhouse.writer.configuration;

import io.arenadata.kafka.clickhouse.writer.verticle.ConfigurableVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Slf4j
@Configuration
public class VertxConfiguration implements ApplicationListener<ApplicationReadyEvent> {
    private static final String DEPLOY_SUCCESS_MSG = "Stream services for [{}] deployed";
    private static final String DEPLOY_ERROR_MSG = "Deployment error";

    @Bean
    public WebClient webClient(Vertx vertx) {
        return WebClient.create(vertx);
    }

    @Bean
    @ConditionalOnMissingBean(Vertx.class)
    public Vertx vertx() {
        return Vertx.vertx();
    }

    /**
     * Centrally sets all verticals strictly after lifting all configurations. Unlike a call
     * initMethod guarantees the deployment order of verticals, since the @PostConstruct phase only works within
     * configurations.
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        Vertx vertx = event.getApplicationContext().getBean(Vertx.class);
        Map<String, ConfigurableVerticle> verticles = event.getApplicationContext().getBeansOfType(ConfigurableVerticle.class);
        log.debug("found verticals: {}", verticles.size());
        verticles.forEach((key, value) -> vertx.deployVerticle(value, value.getDeploymentOptions(), stringAsyncResult -> {
            if (stringAsyncResult.succeeded()) {
                log.debug(DEPLOY_SUCCESS_MSG, key);
            } else {
                log.error(DEPLOY_ERROR_MSG, stringAsyncResult.cause());
            }
        }));
    }
}
