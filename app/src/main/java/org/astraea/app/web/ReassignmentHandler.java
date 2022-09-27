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
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;

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
        admin.addingReplicas(topics).stream()
            .map(AddingReplica::new)
            .collect(Collectors.toUnmodifiableList()));
  }

  static class AddingReplica implements Response {
    final String topicName;
    final int partition;

    final int broker;
    final String dataFolder;

    final long size;

    final long leaderSize;
    final String progress;

    AddingReplica(org.astraea.common.admin.AddingReplica addingReplica) {
      this.topicName = addingReplica.topic();
      this.partition = addingReplica.partition();
      this.broker = addingReplica.broker();
      this.dataFolder = addingReplica.path();
      this.size = addingReplica.size();
      this.leaderSize = addingReplica.leaderSize();
      this.progress = progressInPercentage(leaderSize == 0 ? 0 : size / leaderSize);
    }
  }

  static class Reassignments implements Response {
    final Collection<AddingReplica> addingReplicas;

    Reassignments(Collection<AddingReplica> addingReplicas) {
      this.addingReplicas = addingReplicas;
    }
  }

  // visible for testing
  static String progressInPercentage(double progress) {
    // min(max(progress, 0), 1) is to force valid progress value is 0 < progress < 1
    // in case something goes wrong (maybe compacting cause data size shrinking suddenly)
    return String.format("%.2f%%", min(max(progress, 0), 1) * 100);
  }
}
