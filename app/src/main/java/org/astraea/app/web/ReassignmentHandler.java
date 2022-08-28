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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;

public class ReassignmentHandler implements Handler {
  static final String PLANS_KEY = "plans";
  static final String TOPIC_KEY = "topic";
  static final String PARTITION_KEY = "partition";
  static final String TO_KEY = "to";
  static final String BROKER_KEY = "broker";
  private final Admin admin;

  ReassignmentHandler(Admin admin) {
    this.admin = admin;
  }

  static boolean moveBroker(PostRequest request) {
    // the keys used by moveBroker are subset of moveFolder, so we have to exclude moveFolder first.
    return !moveFolder(request) && request.has(TOPIC_KEY, PARTITION_KEY, TO_KEY);
  }

  static boolean moveFolder(PostRequest request) {
    return request.has(BROKER_KEY, TOPIC_KEY, PARTITION_KEY, TO_KEY);
  }

  static boolean badPost(Channel channel) {
    var rs = channel.request().requests(PLANS_KEY);
    return rs.isEmpty() || !rs.stream().allMatch(r -> moveBroker(r) || moveFolder(r));
  }

  @Override
  public Response post(Channel channel) {
    if (badPost(channel)) return Response.BAD_REQUEST;

    // move node first to avoid UnknownTopicOrPartitionException caused by moving folder later
    var movingPartitions =
        channel.request().requests(PLANS_KEY).stream()
            .filter(ReassignmentHandler::moveBroker)
            .collect(
                Collectors.toMap(
                    request ->
                        TopicPartition.of(
                            request.value(TOPIC_KEY), request.intValue(PARTITION_KEY)),
                    request -> request.intValues(TO_KEY)));

    movingPartitions.forEach(
        (tp, brokers) -> admin.migrator().partition(tp.topic(), tp.partition()).moveTo(brokers));

    // wait for all node moving get start
    // TODO: how to make graceful waiting???
    if (channel.request().requests(PLANS_KEY).stream().anyMatch(ReassignmentHandler::moveFolder)) {
      Utils.waitFor(
          () -> {
            var replicas =
                admin.replicas(
                    movingPartitions.keySet().stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toUnmodifiableSet()));
            return movingPartitions.entrySet().stream()
                .allMatch(
                    entry ->
                        replicas.get(entry.getKey()).stream()
                            .map(r -> r.nodeInfo().id())
                            .collect(Collectors.toUnmodifiableSet())
                            .containsAll(entry.getValue()));
          });
    }

    // ok, all replicas are moving to specify nodes. it is ok to change folder now
    channel.request().requests(PLANS_KEY).stream()
        .filter(ReassignmentHandler::moveFolder)
        .forEach(
            request ->
                admin
                    .migrator()
                    .partition(request.value(TOPIC_KEY), request.intValue(PARTITION_KEY))
                    .moveTo(Map.of(request.intValue(BROKER_KEY), request.value(TO_KEY))));
    return Response.ACCEPT;
  }

  @Override
  public Reassignments get(Channel channel) {
    return new Reassignments(
        admin
            .reassignments(Handler.compare(admin.topicNames(), channel.target()))
            .entrySet()
            .stream()
            .map(e -> new Reassignment(e.getKey(), e.getValue().from(), e.getValue().to()))
            .collect(Collectors.toUnmodifiableList()));
  }

  static class Location implements Response {
    final int broker;
    final String path;

    Location(org.astraea.app.admin.Reassignment.Location location) {
      this.broker = location.broker();
      this.path = location.dataFolder();
    }
  }

  static class Reassignment implements Response {
    final String topicName;
    final int partition;
    final Collection<Location> from;
    final Collection<Location> to;

    Reassignment(
        TopicPartition topicPartition,
        Collection<org.astraea.app.admin.Reassignment.Location> from,
        Collection<org.astraea.app.admin.Reassignment.Location> to) {
      this.topicName = topicPartition.topic();
      this.partition = topicPartition.partition();
      this.from = from.stream().map(Location::new).collect(Collectors.toUnmodifiableList());
      this.to = to.stream().map(Location::new).collect(Collectors.toUnmodifiableList());
    }
  }

  static class Reassignments implements Response {
    final Collection<Reassignment> reassignments;

    Reassignments(Collection<Reassignment> reassignments) {
      this.reassignments = reassignments;
    }
  }
}
