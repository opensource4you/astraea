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
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;

public class BulkChecker {
  public static void main(String[] args) throws IOException, InterruptedException {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) throws IOException, InterruptedException {
    var sizes = new HashMap<String, Long>();
    var count = 0L;
    try (var admin = Admin.of(param.bootstrapServers());
        var consumer =
            Consumer.forTopics(Set.copyOf(param.topics))
                .bootstrapServers(param.bootstrapServers())
                .config(
                    ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                    ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                .build()) {
      var tpsAndOffsets =
          admin
              .clusterInfo(Set.copyOf(param.topics))
              .thenApply(
                  c ->
                      c.replicaLeaders().stream()
                          .map(Replica::topicPartition)
                          .collect(Collectors.toSet()))
              .thenCompose(admin::latestOffsets)
              .toCompletableFuture()
              .join();
      var totalRecords = tpsAndOffsets.values().stream().mapToLong(Long::longValue).sum();
      while (count < totalRecords) {
        var records = consumer.poll(Duration.ofSeconds(3));
        records.forEach(
            r ->
                sizes.put(
                    r.topic(),
                    sizes.getOrDefault(r.topic(), 0L)
                        + r.serializedKeySize()
                        + r.serializedValueSize()));
        count += records.size();
      }
    }
    System.out.println("records=" + count);
    System.out.println(
        "size=" + DataSize.Byte.of(sizes.values().stream().mapToLong(Long::longValue).sum()));
    sizes.forEach((k, v) -> System.out.println(k + "=" + v));
  }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you should check",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;
  }
}
