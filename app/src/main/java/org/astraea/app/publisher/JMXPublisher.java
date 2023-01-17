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
package org.astraea.app.publisher;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.performance.AbstractThread;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;

public class JMXPublisher implements AbstractThread {

  // Broker id and mbean client
  private final Map<Integer, MBeanClient> idMBeanClient = new ConcurrentHashMap<>();

  private final Admin admin;
  private final Producer<byte[], String> producer;

  private final Function<Integer, Integer> idToJmxPort;

  private final ScheduledFuture<?> future;

  public JMXPublisher(
      String bootstrapServer, Function<Integer, Integer> idToJmxPort, Duration interval) {
    this.admin = Admin.of(bootstrapServer);
    this.producer =
        Producer.builder()
            .bootstrapServers(bootstrapServer)
            .valueSerializer(Serializer.STRING)
            .build();
    this.idToJmxPort = idToJmxPort;
    this.future =
        Executors.newScheduledThreadPool(1)
            .scheduleAtFixedRate(this::run, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  // Fetch from mbeanClient and send to inner topic

  public void run() {
    updateNodeInfo(admin.nodeInfos());
    // Synchronously fetch metrics from jmx and produce to internal-topic
    idMBeanClient.forEach(
        (id, client) ->
            CompletableFuture.supplyAsync(() -> client.queryBeans(BeanQuery.all()))
                .thenAccept(
                    beans ->
                        beans.stream()
                            .map(
                                bean ->
                                    Record.builder()
                                        .topic(MetricPublisher.internalTopicName(id))
                                        .key((byte[]) null)
                                        .value(bean.toString())
                                        .build())
                            .forEach(producer::send)));
  }

  public void updateNodeInfo(CompletionStage<List<NodeInfo>> nodeInfos) {
    nodeInfos.thenAccept(
        infos -> {
          // Create jmx client when new node detected
          infos.forEach(
              node -> this.idMBeanClient.computeIfAbsent(node.id(), id -> createMBeanClient(node)));
          // Delete non-exist node and close those MBeanClients
          var idSet = infos.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
          this.idMBeanClient.keySet().stream()
              .filter(id -> !idSet.contains(id))
              .forEach(id -> this.idMBeanClient.remove(id).close());
        });
  }

  @Override
  public void waitForDone() {
    Utils.swallowException(this.future::get);
  }

  @Override
  public boolean closed() {
    return future.isDone();
  }

  public void close() {
    future.cancel(false);
    waitForDone();
    idMBeanClient.forEach((id, client) -> client.close());
    admin.close();
  }

  private MBeanClient createMBeanClient(NodeInfo nodeInfo) {
    var jmxPort = idToJmxPort.apply(nodeInfo.id());
    return MBeanClient.jndi(nodeInfo.host(), jmxPort);
  }
}
