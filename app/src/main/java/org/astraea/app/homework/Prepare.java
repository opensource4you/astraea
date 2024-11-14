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
package org.astraea.app.homework;

import com.beust.jcommander.Parameter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartitionPath;

public class Prepare {

  public static void main(String[] args) {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) {
    try (var admin = Admin.of(param.bootstrapServers())) {
      if (param.topics == null || param.topics.isEmpty()) {
        var cluster =
            admin
                .topicNames(true)
                .thenComposeAsync(admin::clusterInfo)
                .toCompletableFuture()
                .join();
        var brokerSize =
            cluster.brokers().stream()
                .collect(
                    Collectors.toMap(
                        Broker::id,
                        b ->
                            b.topicPartitionPaths().stream()
                                .mapToLong(TopicPartitionPath::size)
                                .sum()));
        System.out.println("id,role,value");
        brokerSize.forEach(
            (id, size) -> System.out.println(id + ",broker," + DataSize.Byte.of(size)));

        var partitionSize =
            cluster.replicas().stream()
                .filter(Replica::isLeader)
                .collect(Collectors.toMap(Replica::topicPartition, Replica::size));
        partitionSize.forEach(
            (tp, size) -> System.out.println(tp + ",partition," + DataSize.Byte.of(size)));

        var brokerAvg =
            brokerSize.values().stream().mapToLong(i -> i).filter(i -> i > 0).average().orElse(0);
        var avedevBroker =
            brokerSize.values().stream()
                .mapToDouble(i -> Math.abs(i - brokerAvg))
                .average()
                .orElse(0.0D);
        System.out.println("avedev,broker," + DataSize.Byte.of((long) avedevBroker));

        var partitionAvg =
            partitionSize.values().stream()
                .mapToLong(i -> i)
                .filter(i -> i > 0)
                .average()
                .orElse(0);
        var avedevPartition =
            partitionSize.values().stream()
                .mapToDouble(i -> Math.abs(i - partitionAvg))
                .average()
                .orElse(0.0D);
        System.out.println("avedev,partition," + DataSize.Byte.of((long) avedevPartition));
        return;
      }
      var brokerIds =
          admin.brokers().toCompletableFuture().join().stream().map(Broker::id).toList();
      Function<Integer, List<Integer>> ids =
          index -> {
            if (index < brokerIds.size()) return List.of(brokerIds.get(index));
            return List.of(brokerIds.get((int) (Math.random() * brokerIds.size())));
          };
      param.topics.forEach(
          topic ->
              admin
                  .creator()
                  .topic(topic)
                  .replicasAssignments(
                      IntStream.range(0, brokerIds.size() + 2)
                          .boxed()
                          .collect(Collectors.toMap(i -> i, ids)))
                  .run()
                  .toCompletableFuture()
                  .join());
      System.out.println("succeed to create topics: " + param.topics);
    }
  }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you subscribed",
        validateWith = StringListField.class,
        listConverter = StringListField.class)
    List<String> topics;
  }
}
