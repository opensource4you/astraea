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
package org.astraea.app.balancer.simulation;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.common.DataRate;

public interface SimulationSnapshot {

  Map<NodeInfo, DataRate> ingress();

  Map<TopicPartitionReplica, String> logs();

  Collection<SimulatedProducer> producers();

  default Summary summary() {
    return () -> SimulationSnapshot.this;
  }

  interface Summary {
    SimulationSnapshot snapshot();

    default long totalTopics() {
      return snapshot().logs().keySet().stream()
          .map(TopicPartitionReplica::topic)
          .distinct()
          .count();
    }

    default long totalPartitions() {
      return snapshot().logs().keySet().stream()
          .map(x -> TopicPartition.of(x.topic(), x.partition()))
          .distinct()
          .count();
    }

    default long totalReplicas() {
      return snapshot().logs().keySet().size();
    }

    default double averageLogDirInUse() {
      return snapshot().logs().entrySet().stream()
          .collect(
              Collectors.groupingBy(
                  e -> e.getKey().brokerId(),
                  Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableSet())))
          .values()
          .stream()
          .mapToInt(Set::size)
          .average()
          .orElse(0);
    }

    default double averageReplicaFactor() {
      return snapshot().logs().keySet().stream()
          .collect(
              Collectors.groupingBy(
                  x -> TopicPartition.of(x.topic(), x.partition()), Collectors.counting()))
          .values()
          .stream()
          .mapToLong(x -> x)
          .average()
          .orElse(0);
    }

    default void writeSummary(PrintWriter summaryWriter) {
      // broker
      summaryWriter.println("[Broker Ingress Summary]");
      snapshot()
          .ingress()
          .forEach((node, rate) -> summaryWriter.printf(" * Broker #%d: %s%n", node.id(), rate));
      summaryWriter.printf(
          " * Max ingress: %s%n",
          snapshot().ingress().values().stream().max(Comparator.comparing(DataRate::byteRate)));
      summaryWriter.printf(
          " * Min ingress: %s%n",
          snapshot().ingress().values().stream().min(Comparator.comparing(DataRate::byteRate)));
      summaryWriter.println("");

      // producer
      long totalProducers = snapshot().producers().size();
      var totalEgress =
          DataRate.Byte.of(
                  (long)
                      (snapshot().ingress().values().stream()
                          .mapToDouble(DataRate::byteRate)
                          .sum()))
              .perSecond();
      summaryWriter.println("[Producer Summary]");
      summaryWriter.printf(" * Producers: %d%n", totalProducers);
      summaryWriter.printf(" * Total Egress Bandwidth: %s%n", totalEgress);
      summaryWriter.println("");

      // log
      long topics = totalTopics();
      long partitions = totalPartitions();
      long totalReplicas = totalReplicas();
      double avgReplicaFactor = averageReplicaFactor();
      double avgLogDirInUse = averageLogDirInUse();
      summaryWriter.println("[Log Summary]");
      summaryWriter.printf(" * Topics: %d%n", topics);
      summaryWriter.printf(" * Partitions: %d%n", partitions);
      summaryWriter.printf(" * Total replicas: %d%n", totalReplicas);
      summaryWriter.printf(" * Avg replica factor: %.5f%n", avgReplicaFactor);
      summaryWriter.printf(" * Avg log dir in use: %.5f%n", avgLogDirInUse);
      summaryWriter.println();
    }
  }
}
