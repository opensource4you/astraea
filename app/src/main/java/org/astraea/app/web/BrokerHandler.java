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
package org.astraea.app.web;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.TopicPartition;

class BrokerHandler implements Handler {

  private final Admin admin;

  BrokerHandler(Admin admin) {
    this.admin = admin;
  }

  Set<Integer> brokers(Optional<String> target) {
    try {
      return Handler.compare(admin.brokerIds(), target.map(Integer::valueOf));
    } catch (NumberFormatException e) {
      throw new NoSuchElementException("the broker id must be number");
    }
  }

  @Override
  public Response get(Channel channel) {
    var ids = brokers(channel.target());
    var brokers =
        admin.brokers().stream()
            .filter(n -> ids.contains(n.id()))
            .map(n -> new Broker(n.id(), admin.topicPartitions(n.id()), n.config()))
            .collect(Collectors.toUnmodifiableList());
    if (channel.target().isPresent() && brokers.size() == 1) return brokers.get(0);
    return new Brokers(brokers);
  }

  static class Topic implements Response {
    final String topic;
    final int partitionCount;

    Topic(String topic, int partitionCount) {
      this.topic = topic;
      this.partitionCount = partitionCount;
    }
  }

  static class Broker implements Response {
    final int id;
    final List<Topic> topics;
    final Map<String, String> configs;

    Broker(int id, Set<TopicPartition> topicPartitions, Config configs) {
      this.id = id;
      this.topics =
          topicPartitions.stream()
              .collect(Collectors.groupingBy(TopicPartition::topic))
              .entrySet()
              .stream()
              .map(e -> new Topic(e.getKey(), e.getValue().size()))
              .collect(Collectors.toUnmodifiableList());
      this.configs = configs.raw();
    }
  }

  static class Brokers implements Response {
    final List<Broker> brokers;

    Brokers(List<Broker> brokers) {
      this.brokers = brokers;
    }
  }
}
