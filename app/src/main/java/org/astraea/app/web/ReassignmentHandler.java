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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;

public class ReassignmentHandler implements Handler {
  static final String PLANS_KEY = "plans";
  static final String TOPIC_KEY = "topic";
  static final String PARTITION_KEY = "partition";
  static final String FROM_KEY = "from";
  static final String TO_KEY = "to";
  static final String BROKER_KEY = "broker";
  private final Admin admin;

  ReassignmentHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Response post(Channel channel) {
    var rs =
        channel.request().requests(PLANS_KEY).stream()
            .map(
                request -> {
                  // case 0: move replica to another folder
                  if (request.has(BROKER_KEY, TOPIC_KEY, PARTITION_KEY, TO_KEY)) {
                    admin
                        .migrator()
                        .partition(request.value(TOPIC_KEY), request.intValue(PARTITION_KEY))
                        .moveTo(Map.of(request.intValue(BROKER_KEY), request.value(TO_KEY)));
                    return Response.ACCEPT;
                  }
                  // case 1: move replica to another broker
                  if (request.has(TOPIC_KEY, PARTITION_KEY, TO_KEY)) {
                    admin
                        .migrator()
                        .partition(request.value(TOPIC_KEY), request.intValue(PARTITION_KEY))
                        .moveTo(request.intValues(TO_KEY));
                    return Response.ACCEPT;
                  }
                  // case 2: move specific broker's (topic's) partitions to others
                  if (request.has(FROM_KEY)) {
                    Predicate<TopicPartition> hasTopicKey =
                        request.has(TOPIC_KEY)
                            ? tp -> Objects.equals(tp.topic(), request.value(TOPIC_KEY))
                            : tp -> true;
                    var brokerList =
                        admin.brokerIds().stream()
                            .filter(i -> i != request.intValue(FROM_KEY))
                            .collect(Collectors.toList());
                    Iterator<Integer> it = brokerList.iterator();
                    for (TopicPartition tp :
                        admin.partitions(request.intValue(FROM_KEY)).stream()
                            .filter(hasTopicKey)
                            .collect(Collectors.toList())) {
                      admin
                          .migrator()
                          .partition(tp.topic(), tp.partition())
                          .moveTo(
                              List.of(
                                  it.hasNext() ? it.next() : (it = brokerList.iterator()).next()));
                    }
                    return Response.ACCEPT;
                  }
                  return Response.BAD_REQUEST;
                })
            .collect(Collectors.toUnmodifiableList());
    if (!rs.isEmpty() && rs.stream().allMatch(r -> r == Response.ACCEPT)) return Response.ACCEPT;
    return Response.BAD_REQUEST;
  }

  @Override
  public Reassignments get(Channel channel) {
    var topics = Handler.compare(admin.topicNames(), channel.target());
    var replicas = admin.replicas(topics);
    return new Reassignments(
        admin.reassignments(topics).entrySet().stream()
            .map(
                e ->
                    toReassignment(
                        e.getKey(),
                        e.getValue().from(),
                        e.getValue().to(),
                        replicas.get(e.getKey())))
            .collect(Collectors.toUnmodifiableList()));
  }

  static class Location implements Response {
    final int broker;
    final String path;
    final long size;

    Location(int broker, String path, long size) {
      this.broker = broker;
      this.path = path;
      this.size = size;
    }
  }

  static class Reassignment implements Response {
    final String topicName;
    final int partition;
    final Collection<Location> from;
    final Collection<Location> to;
    final String progress;

    Reassignment(
        TopicPartition topicPartition, Collection<Location> from, Collection<Location> to) {
      this.topicName = topicPartition.topic();
      this.partition = topicPartition.partition();
      this.from = from;
      this.to = to;

      double fromSizeSum = from.stream().map(f -> f.size).reduce(0L, Long::sum);
      double toSizeSum = to.stream().map(t -> t.size).reduce(0L, Long::sum);
      double progress = fromSizeSum == 0 ? 0 : toSizeSum / fromSizeSum;
      this.progress = progressInPercentage(progress);
    }
  }

  static class Reassignments implements Response {
    final Collection<Reassignment> reassignments;

    Reassignments(Collection<Reassignment> reassignments) {
      this.reassignments = reassignments;
    }
  }

  // visible for testing
  static Reassignment toReassignment(
      TopicPartition topicPartition,
      Collection<org.astraea.common.admin.Reassignment.Location> from,
      Collection<org.astraea.common.admin.Reassignment.Location> to,
      Collection<Replica> replicas) {
    return new Reassignment(
        topicPartition,
        from.stream()
            .map(l -> new Location(l.broker(), l.dataFolder(), findReplica(replicas, l).size()))
            .collect(Collectors.toUnmodifiableList()),
        to.stream()
            .map(l -> new Location(l.broker(), l.dataFolder(), findReplica(replicas, l).size()))
            .collect(Collectors.toUnmodifiableList()));
  }

  // visible for testing
  static String progressInPercentage(double progress) {
    // min(max(progress, 0), 1) is to force valid progress value is 0 < progress < 1
    // in case something goes wrong (maybe compacting cause data size shrinking suddenly)
    return String.format("%.2f%%", min(max(progress, 0), 1) * 100);
  }

  private static Replica findReplica(
      Collection<Replica> replicas, org.astraea.common.admin.Reassignment.Location location) {
    return replicas.stream()
        .filter(
            r ->
                r.nodeInfo().id() == location.broker()
                    && r.dataFolder().equals(location.dataFolder()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("replica not found"));
  }
}
