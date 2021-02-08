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
package io.arenadata.kafka.clickhouse.reader.configuration;

import io.arenadata.kafka.clickhouse.reader.configuration.properties.KafkaProperties;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletableFuture;

@Configuration
@ConditionalOnProperty(value = "vertx.clustered", havingValue = "true")
public class VertxClusterConfiguration {

    @Bean
    public Vertx clusteredVertx(ZookeeperClusterManager clusterManager) {
        VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
        CompletableFuture<Vertx> future = new CompletableFuture<>();
        Vertx.clusteredVertx(options, event -> {
            if (event.succeeded()) {
                future.complete(event.result());
            } else {
                future.complete(null);
            }
        });
        return future.join();
    }

    @Bean
    public ZookeeperClusterManager clusterManager(KafkaProperties properties) {
        JsonObject config = new JsonObject();
        config.put("zookeeperHosts", properties.getCluster().getZookeeperHosts());
        config.put("rootPath", properties.getCluster().getRootPath());
        config.put("retry", new JsonObject()
                .put("initialSleepTime", 3000)
                .put("maxTimes", 3)
        );
        return new ZookeeperClusterManager(config);
    }
}
