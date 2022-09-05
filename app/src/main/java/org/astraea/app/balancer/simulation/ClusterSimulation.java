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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.simulation.events.ClusterEvent;
import org.astraea.app.balancer.simulation.events.NewProducerEvent;
import org.astraea.app.balancer.simulation.events.StopProducerEvent;
import org.astraea.app.balancer.simulation.events.TopicCreationEvent;
import org.astraea.app.balancer.simulation.events.TopicDeletionEvent;
import org.astraea.app.common.DataRate;

public interface ClusterSimulation {

  /**
   * Affect the simulation environment by the given {@link ClusterEvent}. Executing an event might
   * make the time of the simulation environment increase.
   */
  void execute(ClusterEvent event);

  /** Create a snapshot of the simulated cluster. */
  SimulationSnapshot snapshot();

  /**
   * Use an actual Kafka cluster to help some simulation processes. This simulation might mutate the
   * state of the cluster. So you should use it for a testing cluster environment only.
   *
   * @param admin the {@link Admin} point to the testing cluster environment.
   */
  static ClusterSimulation pseudo(Admin admin) {
    // current implementation use the partition/replica/log-dir allocation algorithm inside kafka.
    return new ClusterSimulation() {
      private long systemTime = 0;
      private final List<SimulatedProducer> producers = new ArrayList<>();

      @Override
      public void execute(ClusterEvent event) {
        // update the system time
        if (event.eventTime() < systemTime)
          throw new IllegalStateException("Can't process old event");
        systemTime = event.eventTime();

        // consume event
        if (event instanceof TopicCreationEvent) createTopic((TopicCreationEvent) event);
        else if (event instanceof TopicDeletionEvent) deleteTopic((TopicDeletionEvent) event);
        else if (event instanceof NewProducerEvent) addProducer((NewProducerEvent) event);
        else if (event instanceof StopProducerEvent) stopProducer((StopProducerEvent) event);
        else throw new IllegalArgumentException("Can't process " + event.getClass().getName());
      }

      @Override
      public SimulationSnapshot snapshot() {
        final var replicas = admin.replicas();
        final var leaders =
            replicas.values().stream()
                .flatMap(Collection::stream)
                .filter(ReplicaInfo::isLeader)
                .collect(
                    Collectors.groupingBy(
                        ReplicaInfo::nodeInfo,
                        Collectors.mapping(
                            ReplicaInfo::topicPartition, Collectors.toUnmodifiableSet())));
        final var ingress =
            producers.stream()
                .map(producer -> producer.ingress(leaders, systemTime))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(
                    Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(
                            Map.Entry::getValue,
                            Collectors.reducing(
                                DataRate.ZERO,
                                x -> x,
                                (DataRate a, DataRate b) ->
                                    DataRate.Byte.of((long) (a.byteRate() + b.byteRate()))
                                        .perSecond()))));
        final var allocation =
            replicas.values().stream()
                .flatMap(Collection::stream)
                .collect(
                    Collectors.toUnmodifiableMap(
                        ReplicaInfo::topicPartitionReplica, Replica::dataFolder));
        final var producers = List.copyOf(this.producers);

        return new SimulationSnapshot() {
          @Override
          public Map<NodeInfo, DataRate> ingress() {
            return ingress;
          }

          @Override
          public Map<TopicPartitionReplica, String> logs() {
            return allocation;
          }

          @Override
          public Collection<SimulatedProducer> producers() {
            return producers;
          }
        };
      }

      private void createTopic(TopicCreationEvent event) {
        admin
            .creator()
            .topic(event.topicName())
            .numberOfPartitions(event.partitionSize())
            .numberOfReplicas(event.replicaSize())
            .create();
      }

      private void deleteTopic(TopicDeletionEvent event) {
        admin.deleteTopics(Set.of(event.topicName()));
      }

      private void addProducer(NewProducerEvent event) {
        producers.add(event.toSimulatedProducer());
      }

      private void stopProducer(StopProducerEvent event) {
        producers.removeIf(producer -> producer.event().producerId().equals(event.producerId()));
      }
    };
  }
}
