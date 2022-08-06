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
package org.astraea.app.partitioner;

import java.lang.reflect.Field;
import java.security.Key;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.stats.Value;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.NodeTopicSizeCost;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Receiver;

public interface Dispatcher extends Partitioner {
  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node. For example:
   * var dispatch = Dispatcher.of(producer); dispatch.startInterdependent(); producer.send();
   * dispatch.endInterdependent();
   *
   * @param producer Kafka producer
   * @return The dispatch of Kafka Producer
   */
  static Dispatcher of(Producer<Key, Value> producer) {
    try {
      Field field = producer.getClass().getDeclaredField("partitioner");
      field.setAccessible(true);
      var dispatcher = (Dispatcher) field.get(producer);
      interdependent.jmxAddress = dispatcher.jmxAddress();
      return (Dispatcher) field.get(producer);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * cache the cluster info to reduce the cost of converting cluster. Producer does not update
   * Cluster frequently, so it is ok to cache it.
   */
  ConcurrentHashMap<Cluster, ClusterInfo> CLUSTER_CACHE = new ConcurrentHashMap<>();

  Interdependent interdependent = new Interdependent();

  NodeTopicSizeCost nodeTopicSizeCost = new NodeTopicSizeCost();
  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on
   * @param value The value to partition
   * @param clusterInfo The current cluster metadata
   */
  int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo);

  Function<Integer, Optional<Integer>> jmxAddress();
  /**
   * configure this dispatcher. This method is called only once.
   *
   * @param config configuration
   */
  default void configure(Configuration config) {}

  /** close this dispatcher. This method is executed only once. */
  @Override
  default void close() {}

  @Override
  default void configure(Map<String, ?> configs) {
    configure(
        Configuration.of(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()))));
  }

  @Override
  default int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return interdependent.isInterdependent
        ? interdependent.targetPartition(
            topic, CLUSTER_CACHE.computeIfAbsent(cluster, ignored -> ClusterInfo.of(cluster)))
        : partition(
            topic,
            keyBytes == null ? new byte[0] : keyBytes,
            valueBytes == null ? new byte[0] : valueBytes,
            CLUSTER_CACHE.computeIfAbsent(cluster, ignored -> ClusterInfo.of(cluster)));
  }

  @Override
  default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}

  /** Begin interdependence function.Let the next messages be interdependent. */
  default void beginInterdependent() {
    interdependent.isFirst = !interdependent.isInterdependent;
    interdependent.isInterdependent = true;
  }

  /** Close interdependence function.Send data using the original Dispatcher logic. */
  default void endInterdependent() {
    interdependent.isInterdependent = false;
    interdependent.isFirst = false;
  }

  class Interdependent {
    private final Map<Integer, Receiver> receivers = new TreeMap<>();
    private Function<Integer, Optional<Integer>> jmxAddress;
    private boolean isInterdependent = false;
    private boolean isFirst = false;
    private int targetPartition = -1;

    int targetPartition(String topic, ClusterInfo clusterInfo) {
      return isFirst ? bestPartition(topic, clusterInfo) : targetPartition;
    }

    int bestPartition(String topic, ClusterInfo clusterInfo) {
      clusterInfo.availableReplicas(topic).stream()
          .filter(p -> !receivers.containsKey(p.nodeInfo().id()))
          .forEach(
              p ->
                  receivers.put(
                      p.nodeInfo().id(),
                      receiver(
                          p.nodeInfo().host(),
                          jmxAddress
                              .apply(p.nodeInfo().id())
                              .orElseThrow(() -> new RuntimeException("No match jmx port.")))));
      var beans =
          receivers.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));
      var targetBroker =
          nodeTopicSizeCost
              .brokerCost(clusterInfo, ClusterBean.of(beans))
              .value()
              .entrySet()
              .stream()
              .min(Comparator.comparingDouble(Map.Entry::getValue))
              .orElse(Map.entry(0, 0.0))
              .getKey();
      targetPartition =
          nodeTopicSizeCost
              .partitionCost(clusterInfo, ClusterBean.of(beans))
              .value(targetBroker)
              .entrySet()
              .stream()
              .filter(entry -> entry.getKey().topic().equals(topic))
              .min(Comparator.comparingDouble(Map.Entry::getValue))
              .orElseThrow(
                  () ->
                      new RuntimeException(
                          "No partition score,please check that JMX metrics have been fetched."))
              .getKey()
              .partition();
      close();
      return targetPartition;
    }

    Receiver receiver(String host, int port) {
      var beanCollector = BeanCollector.builder().clientCreator(MBeanClient::jndi).build();
      return beanCollector
          .register()
          .host(host)
          .port(port)
          // TODO: handle the empty fetcher
          .fetcher(nodeTopicSizeCost.fetcher().get())
          .build();
    }

    private void close() {
      receivers.values().forEach(r -> Utils.swallowException(r::close));
      receivers.clear();
      isFirst = false;
    }
  }
}
