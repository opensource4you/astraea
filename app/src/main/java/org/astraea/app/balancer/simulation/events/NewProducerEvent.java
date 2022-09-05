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
package org.astraea.app.balancer.simulation.events;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.simulation.SimulatedProducer;
import org.astraea.app.common.DataRate;

public interface NewProducerEvent extends ClusterEvent {

  static NewProducerEvent of(
      long time, String producerId, Set<TopicPartition> targets, DataRate rate) {
    return new NewProducerEvent() {
      final Set<TopicPartition> copy = Set.copyOf(targets);

      @Override
      public String producerId() {
        return producerId;
      }

      @Override
      public Set<TopicPartition> targets() {
        return copy;
      }

      @Override
      public DataRate throughput() {
        return rate;
      }

      @Override
      public long eventTime() {
        return time;
      }
    };
  }

  String producerId();

  // TODO: revise this usage once https://github.com/skiptests/astraea/issues/669 is supported.
  Set<TopicPartition> targets();

  // TODO: revise this usage once https://github.com/skiptests/astraea/issues/669 is supported.
  DataRate throughput();

  default SimulatedProducer toSimulatedProducer() {
    return new SimulatedProducerImpl(this);
  }

  class Serialize implements JsonSerializer<SimulatedProducer> {

    @Override
    public JsonElement serialize(
        SimulatedProducer src, Type typeOfSrc, JsonSerializationContext context) {
      final var object = new JsonObject();
      final var event = src.event();
      object.add(
          "targets",
          context.serialize(
              event.targets().stream()
                  .map(TopicPartition::toString)
                  .collect(Collectors.joining(","))));
      object.add("throughput", context.serialize((long) event.throughput().byteRate()));
      return object;
    }
  }

  class SimulatedProducerImpl implements SimulatedProducer {

    private final NewProducerEvent event;

    public SimulatedProducerImpl(NewProducerEvent event) {
      this.event = event;
    }

    @Override
    public Map<NodeInfo, DataRate> ingress(Map<NodeInfo, Set<TopicPartition>> leaders, long time) {
      double throughputPerTarget = event().throughput().byteRate() / event().targets().size();
      return leaders.entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  Map.Entry::getKey,
                  entry ->
                      DataRate.Byte.of(
                              (long)
                                  (entry.getValue().stream()
                                          .filter(tp -> event().targets().contains(tp))
                                          .count()
                                      * throughputPerTarget))
                          .perSecond()));
    }

    @Override
    public NewProducerEvent event() {
      return event;
    }
  }
}
