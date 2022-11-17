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
package org.astraea.app.backup;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.argument.Argument;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Header;
import org.astraea.common.consumer.Record;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestImportExport extends RequireBrokerCluster {

  @Test
  void test() throws IOException {
    var records = 30;
    var topics = Set.of(Utils.randomString(), Utils.randomString(), Utils.randomString());
    var start = System.currentTimeMillis();
    try (var producer = Producer.of(bootstrapServers())) {
      topics.forEach(
          t ->
              IntStream.range(0, records)
                  .forEach(
                      i ->
                          producer
                              .sender()
                              .topic(t)
                              .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                              .value(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                              .headers(
                                  List.of(
                                      Header.of(
                                          String.valueOf(i),
                                          String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                              .timestamp(start + i)
                              .run()));
    }
    var group = Utils.randomString();
    var file = Files.createTempDirectory("test_import_export");

    // test export
    var exportArg =
        Argument.parse(
            new Exporter.Argument(),
            new String[] {
              "--bootstrap.servers", bootstrapServers(),
              "--topics", String.join(",", topics),
              "--output", file.toString(),
              "--group", group
            });
    var exportResult = Exporter.execute(exportArg);
    Assertions.assertEquals(topics.size(), exportResult.recordCount().size());
    exportResult.recordCount().values().forEach(v -> Assertions.assertEquals(records, v));
    // TODO: we should create the folder for each partition
    Assertions.assertEquals(
        topics.size(),
        Arrays.stream(Objects.requireNonNull(file.toFile().listFiles()))
            .filter(File::isFile)
            .count());

    // use the same group and there is no more records
    Assertions.assertEquals(0, Exporter.execute(exportArg).recordCount().size());

    // cleanup topics
    try (var admin = Admin.of(bootstrapServers())) {
      admin.deleteTopics(topics).toCompletableFuture().join();
    }

    // test import
    var importArg =
        Argument.parse(
            new Importer.Argument(),
            new String[] {
              "--bootstrap.servers", bootstrapServers(),
              "--input", file.toString()
            });
    var importResult = Importer.execute(importArg);
    Assertions.assertEquals(topics.size(), importResult.recordCount().size());
    importResult.recordCount().values().forEach(v -> Assertions.assertEquals(records, v));
    try (var admin = Admin.of(bootstrapServers())) {
      var offsets =
          admin
              .latestOffsets(admin.topicPartitions(topics).toCompletableFuture().join())
              .toCompletableFuture()
              .join();
      Assertions.assertEquals(topics.size(), offsets.size());
      offsets.values().forEach(v -> Assertions.assertEquals(records, v));
    }
    try (var consumer =
        Consumer.forTopics(topics)
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      var fetched = consumer.poll(records * topics.size(), Duration.ofSeconds(60));
      Assertions.assertEquals(records * topics.size(), fetched.size());
      var groups = fetched.stream().collect(Collectors.groupingBy(Record::topic));
      Assertions.assertEquals(topics.size(), groups.size());
      groups
          .values()
          .forEach(
              rs -> {
                Assertions.assertEquals(records, rs.size());
                for (var i = 0; i < records; ++i) {
                  Assertions.assertEquals(
                      String.valueOf(i), new String(rs.get(i).key(), StandardCharsets.UTF_8));
                  Assertions.assertEquals(
                      String.valueOf(i), new String(rs.get(i).value(), StandardCharsets.UTF_8));
                  var headers = rs.get(i).headers();
                  Assertions.assertEquals(1, headers.size());
                  Assertions.assertEquals(String.valueOf(i), headers.iterator().next().key());
                  Assertions.assertEquals(
                      String.valueOf(i),
                      new String(headers.iterator().next().value(), StandardCharsets.UTF_8));
                  Assertions.assertEquals(start + i, rs.get(i).timestamp());
                }
              });
    }
  }
}
