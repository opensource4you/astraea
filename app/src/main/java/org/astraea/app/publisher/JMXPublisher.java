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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;

public class JMXPublisher implements Publisher, AutoCloseable {

  // Broker id and mbean client
  private final Map<Integer, MBeanClient> idMBeanClient = new ConcurrentHashMap<>();

  private final Producer<byte[], String> producer;

  private final Function<Integer, Integer> idToJmxPort;

  public JMXPublisher(
      String bootstrapServer, List<NodeInfo> clusterInfo, Function<Integer, Integer> idToJmxPort) {
    producer =
        Producer.builder()
            .bootstrapServers(bootstrapServer)
            .valueSerializer(Serializer.STRING)
            .build();
    this.idToJmxPort = idToJmxPort;
    updateNodeInfo(clusterInfo);
  }

  // Fetch from mbeanClient and send to inner topic
  @Override
  public void run() {
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

  @Override
  public void updateNodeInfo(List<NodeInfo> nodeInfos) {
    // Create jmx client when new node detected
    nodeInfos.forEach(
        node -> this.idMBeanClient.computeIfAbsent(node.id(), id -> createMBeanClient(node)));
    // Delete non-exist node and close those MBeanClients
    var idSet = nodeInfos.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
    this.idMBeanClient.keySet().stream()
        .filter(id -> !idSet.contains(id))
        .forEach(id -> this.idMBeanClient.remove(id).close());
  }

  @Override
  public void close() {
    idMBeanClient.forEach((id, client) -> client.close());
  }

  private MBeanClient createMBeanClient(NodeInfo nodeInfo) {
    var jmxPort = idToJmxPort.apply(nodeInfo.id());
    return MBeanClient.jndi(nodeInfo.host(), jmxPort);
  }
}
