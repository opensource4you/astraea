/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.metrics.collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.JndiClient;

public class ServerMetricFetcher implements MetricsReporter, ClientTelemetry {
  private static final String BOOTSTRAP_SERVERS_CONFIG = "server.metric.fetcher.bootstrap.servers";
  private static final String INTERVAL_CONFIG = "server.metric.fetcher.interval.seconds";
  private String bootstrapServers;
  private int nodeId = -1;
  private Duration interval = Duration.ofSeconds(10);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final BlockingQueue<Boolean> queue = new LinkedBlockingQueue<>(1);

  @Override
  public void init(List<KafkaMetric> list) {}

  @Override
  public void metricChange(KafkaMetric kafkaMetric) {}

  @Override
  public void metricRemoval(KafkaMetric kafkaMetric) {}

  @Override
  public void close() {
    closed.set(true);
    queue.offer(true);
  }

  @Override
  public void configure(Map<String, ?> map) {
    if (!map.containsKey(BOOTSTRAP_SERVERS_CONFIG))
      throw new RuntimeException(BOOTSTRAP_SERVERS_CONFIG + " is required");
    if (!map.containsKey("node.id")) throw new RuntimeException("node.id is required");
    this.bootstrapServers = map.get(BOOTSTRAP_SERVERS_CONFIG).toString();
    nodeId = Integer.parseInt(map.get("node.id").toString());
    if (map.containsKey(INTERVAL_CONFIG))
      interval = Duration.ofSeconds(Long.parseLong(map.get(INTERVAL_CONFIG).toString()));
    CompletableFuture.runAsync(
        () -> {
          MetricSender sender = null;
          var lastSent = System.currentTimeMillis();
          try {
            while (!closed.get()) {
              var done = queue.poll(interval.toSeconds(), TimeUnit.SECONDS);
              if (done == null) done = false;
              if (done) return;
              if (System.currentTimeMillis() - lastSent <= interval.toMillis()) continue;
              if (sender == null) sender = MetricSender.topic(bootstrapServers);
              var beans = JndiClient.local().beans(BeanQuery.all());
              sender.send(nodeId, beans);
              lastSent = System.currentTimeMillis();
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } finally {
            if (sender != null) sender.close();
          }
        });
  }

  @Override
  public ClientTelemetryReceiver clientReceiver() {
    return (__, ___) -> {
      queue.offer(false);
    };
  }
}
