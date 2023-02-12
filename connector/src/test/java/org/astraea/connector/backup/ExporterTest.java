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
package org.astraea.connector.backup;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.connector.Config;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.connector.Value;
import org.astraea.common.consumer.Record;
import org.astraea.common.producer.Producer;
import org.astraea.fs.FileSystem;
import org.astraea.it.FtpServer;
import org.astraea.it.HdfsServer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExporterTest {

  private static final Service SERVICE =
      Service.builder().numberOfWorkers(1).numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testRunWithDefaultConfigs() throws IOException {
    var topic = Utils.randomString();
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    try (var producer = Producer.of(SERVICE.bootstrapServers())) {
      IntStream.range(0, 100)
          .forEach(
              i ->
                  producer.send(
                      org.astraea.common.producer.Record.builder()
                          .topic(topic)
                          .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                          .value(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                          .build()));
      producer.flush();
    }

    var tmpFolder = Files.createTempDirectory("testRunWithDefaultConfigs").toString();
    var name = Utils.randomString();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                Exporter.class.getName(),
                ConnectorConfigs.TOPICS_KEY,
                topic,
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                "path",
                tmpFolder,
                "fs.schema",
                "local"))
        .toCompletableFuture()
        .join();

    Utils.sleep(Duration.ofSeconds(3));

    var status = client.connectorStatus(name).toCompletableFuture().join();
    Assertions.assertEquals("RUNNING", status.state());
    Assertions.assertNotEquals(0, status.tasks().size());
    status
        .tasks()
        .forEach(t -> Assertions.assertEquals("RUNNING", t.state(), t.error().orElse("")));
  }

  @Test
  void testRequiredConfigs() {
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var validation =
        client
            .validate(Exporter.class.getName(), Map.of("topics", "aa", "name", "b"))
            .toCompletableFuture()
            .join();
    Assertions.assertNotEquals(0, validation.errorCount());

    var failed =
        validation.configs().stream()
            .map(Config::value)
            .filter(v -> !v.errors().isEmpty())
            .collect(Collectors.toMap(Value::name, Function.identity()));
    Assertions.assertEquals(
        Set.of(Exporter.SCHEMA_KEY.name(), Exporter.PATH_KEY.name()), failed.keySet());
  }

  @Test
  void testCreateFtpSinkConnector() {

    var topicName = Utils.randomString(10);

    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    Map<String, String> connectorConfigs =
        Map.of(
            "fs.schema",
            "ftp",
            "connector.class",
            Exporter.class.getName(),
            "tasks.max",
            "2",
            "topics",
            topicName,
            "fs.ftp.hostname",
            "127.0.0.1",
            "fs.ftp.port",
            "21",
            "fs.ftp.user",
            "user",
            "fs.ftp.password",
            "password",
            "path",
            "/");

    var createdConnectorInfo =
        connectorClient.createConnector("FtpSink", connectorConfigs).toCompletableFuture().join();

    var configs = createdConnectorInfo.config();

    Assertions.assertEquals("FtpSink", createdConnectorInfo.name());
    Assertions.assertEquals("2", configs.get("tasks.max"));
    Assertions.assertEquals(Exporter.class.getName(), configs.get("connector.class"));
  }

  @Test
  void testFtpSinkTask() {
    try (var server = FtpServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new Exporter.Task();
      var configs =
          Map.of(
              "fs.schema",
              "ftp",
              "topics",
              topicName,
              "fs.ftp.hostname",
              String.valueOf(server.hostname()),
              "fs.ftp.port",
              String.valueOf(server.port()),
              "fs.ftp.user",
              String.valueOf(server.user()),
              "fs.ftp.password",
              String.valueOf(server.password()),
              "path",
              "/" + fileSize,
              "size",
              fileSize,
              "tasks.max",
              "1",
              "roll.duration",
              "100m");

      var fs = FileSystem.of("ftp", Configuration.of(configs));

      task.start(configs);

      var records =
          List.of(
              Record.builder()
                  .topic(topicName)
                  .key("test".getBytes())
                  .value("test0".getBytes())
                  .partition(0)
                  .timestamp(System.currentTimeMillis())
                  .build(),
              Record.builder()
                  .topic(topicName)
                  .key("test".getBytes())
                  .value("test1".getBytes())
                  .partition(1)
                  .timestamp(System.currentTimeMillis())
                  .build());

      task.put(records);

      Utils.sleep(Duration.ofMillis(2000));

      task.close();

      Assertions.assertTrue(task.isWriterDone());

      Assertions.assertEquals(
          2, fs.listFolders("/" + String.join("/", fileSize, topicName)).size());

      records.forEach(
          sinkRecord -> {
            var input =
                fs.read(
                    "/"
                        + String.join(
                            "/",
                            fileSize,
                            topicName,
                            String.valueOf(sinkRecord.partition()),
                            String.valueOf(sinkRecord.offset())));
            var reader = RecordReader.builder(input).build();

            while (reader.hasNext()) {
              var record = reader.next();
              Assertions.assertArrayEquals(record.key(), sinkRecord.key());
              Assertions.assertArrayEquals(record.value(), sinkRecord.value());
              Assertions.assertEquals(record.topic(), sinkRecord.topic());
              Assertions.assertEquals(record.partition(), sinkRecord.partition());
              Assertions.assertEquals(record.timestamp(), sinkRecord.timestamp());
            }
          });
    }
  }

  @Test
  void testFtpSinkTaskIntervalWith1File() {
    try (var server = FtpServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new Exporter.Task();
      var configs =
          Map.of(
              "fs.schema",
              "ftp",
              "topics",
              topicName,
              "fs.ftp.hostname",
              String.valueOf(server.hostname()),
              "fs.ftp.port",
              String.valueOf(server.port()),
              "fs.ftp.user",
              String.valueOf(server.user()),
              "fs.ftp.password",
              String.valueOf(server.password()),
              "path",
              "/" + fileSize,
              "size",
              fileSize,
              "tasks.max",
              "1",
              "roll.duration",
              "300ms");

      var fs = FileSystem.of("ftp", Configuration.of(configs));

      task.start(configs);

      var records1 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test0".getBytes())
              .partition(0)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build();

      task.put(List.of(records1));

      Utils.sleep(Duration.ofMillis(1000));

      Assertions.assertEquals(
          1, fs.listFiles("/" + String.join("/", fileSize, topicName, "0")).size());

      var input =
          fs.read(
              "/"
                  + String.join(
                      "/",
                      fileSize,
                      topicName,
                      String.valueOf(records1.partition()),
                      String.valueOf(records1.offset())));
      var reader = RecordReader.builder(input).build();

      while (reader.hasNext()) {
        var record = reader.next();
        Assertions.assertArrayEquals(record.key(), records1.key());
        Assertions.assertArrayEquals(record.value(), records1.value());
        Assertions.assertEquals(record.topic(), records1.topic());
        Assertions.assertEquals(record.partition(), records1.partition());
        Assertions.assertEquals(record.timestamp(), records1.timestamp());
        Assertions.assertEquals(record.offset(), records1.offset());
      }
      task.close();
    }
  }

  @Test
  void testFtpSinkTaskIntervalWith2Writers() {
    try (var server = FtpServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new Exporter.Task();
      var configs =
          Map.of(
              "fs.schema",
              "ftp",
              "topics",
              topicName,
              "fs.ftp.hostname",
              String.valueOf(server.hostname()),
              "fs.ftp.port",
              String.valueOf(server.port()),
              "fs.ftp.user",
              String.valueOf(server.user()),
              "fs.ftp.password",
              String.valueOf(server.password()),
              "path",
              "/" + fileSize,
              "size",
              fileSize,
              "tasks.max",
              "1",
              "roll.duration",
              "100ms");

      var fs = FileSystem.of("ftp", Configuration.of(configs));

      task.start(configs);

      var record1 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test0".getBytes())
              .partition(0)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build();

      var record2 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test1".getBytes())
              .partition(1)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build();

      var record3 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test2".getBytes())
              .partition(0)
              .offset(1)
              .timestamp(System.currentTimeMillis())
              .build();

      task.put(List.of(record1));
      Utils.sleep(Duration.ofMillis(500));

      task.put(List.of(record2));
      Utils.sleep(Duration.ofMillis(1000));

      task.put(List.of(record3));
      Utils.sleep(Duration.ofMillis(1000));

      Assertions.assertEquals(
          2, fs.listFolders("/" + String.join("/", fileSize, topicName)).size());

      Assertions.assertEquals(
          2, fs.listFiles("/" + String.join("/", fileSize, topicName, "0")).size());

      List.of(record1, record2, record3)
          .forEach(
              sinkRecord -> {
                var input =
                    fs.read(
                        "/"
                            + String.join(
                                "/",
                                fileSize,
                                topicName,
                                String.valueOf(sinkRecord.partition()),
                                String.valueOf(sinkRecord.offset())));
                var reader = RecordReader.builder(input).build();

                while (reader.hasNext()) {
                  var record = reader.next();
                  Assertions.assertArrayEquals(record.key(), sinkRecord.key());
                  Assertions.assertArrayEquals(record.value(), sinkRecord.value());
                  Assertions.assertEquals(record.topic(), sinkRecord.topic());
                  Assertions.assertEquals(record.partition(), sinkRecord.partition());
                  Assertions.assertEquals(record.timestamp(), sinkRecord.timestamp());
                  Assertions.assertEquals(record.offset(), sinkRecord.offset());
                }
              });
      task.close();
    }
  }

  @Test
  void testHdfsSinkTask() {
    try (var server = HdfsServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new Exporter.Task();
      var configs =
          Map.of(
              "fs.schema",
              "hdfs",
              "topics",
              topicName,
              "fs.hdfs.hostname",
              String.valueOf(server.hostname()),
              "fs.hdfs.port",
              String.valueOf(server.port()),
              "fs.hdfs.user",
              String.valueOf(server.user()),
              "path",
              "/" + fileSize,
              "size",
              fileSize,
              "tasks.max",
              "1",
              "roll.duration",
              "100m",
              "fs.hdfs.override.dfs.client.use.datanode.hostname",
              "true");

      task.start(configs);

      var records =
          List.of(
              Record.builder()
                  .topic(topicName)
                  .key("test".getBytes())
                  .value("test0".getBytes())
                  .partition(0)
                  .timestamp(System.currentTimeMillis())
                  .build(),
              Record.builder()
                  .topic(topicName)
                  .key("test".getBytes())
                  .value("test1".getBytes())
                  .partition(1)
                  .timestamp(System.currentTimeMillis())
                  .build());

      task.put(records);

      Utils.sleep(Duration.ofMillis(2000));

      task.close();

      Assertions.assertTrue(task.isWriterDone());

      var fs = FileSystem.of("hdfs", Configuration.of(configs));

      Assertions.assertEquals(
          2, fs.listFolders("/" + String.join("/", fileSize, topicName)).size());

      records.forEach(
          sinkRecord -> {
            var input =
                fs.read(
                    "/"
                        + String.join(
                            "/",
                            fileSize,
                            topicName,
                            String.valueOf(sinkRecord.partition()),
                            String.valueOf(sinkRecord.offset())));
            var reader = RecordReader.builder(input).build();

            while (reader.hasNext()) {
              var record = reader.next();
              Assertions.assertArrayEquals(record.key(), sinkRecord.key());
              Assertions.assertArrayEquals(record.value(), sinkRecord.value());
              Assertions.assertEquals(record.topic(), sinkRecord.topic());
              Assertions.assertEquals(record.partition(), sinkRecord.partition());
              Assertions.assertEquals(record.timestamp(), sinkRecord.timestamp());
            }
          });
    }
  }

  @Test
  void testHdfsSinkTaskIntervalWith1File() {
    try (var server = HdfsServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new Exporter.Task();
      var configs =
          Map.of(
              "fs.schema",
              "hdfs",
              "topics",
              topicName,
              "fs.hdfs.hostname",
              String.valueOf(server.hostname()),
              "fs.hdfs.port",
              String.valueOf(server.port()),
              "fs.hdfs.user",
              String.valueOf(server.user()),
              "path",
              "/" + fileSize,
              "size",
              fileSize,
              "tasks.max",
              "1",
              "roll.duration",
              "300ms");

      task.start(configs);

      var records1 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test0".getBytes())
              .partition(0)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build();

      task.put(List.of(records1));

      Utils.sleep(Duration.ofMillis(1000));

      var fs = FileSystem.of("hdfs", Configuration.of(configs));

      Assertions.assertEquals(
          1, fs.listFiles("/" + String.join("/", fileSize, topicName, "0")).size());

      var input =
          fs.read(
              "/"
                  + String.join(
                      "/",
                      fileSize,
                      topicName,
                      String.valueOf(records1.partition()),
                      String.valueOf(records1.offset())));
      var reader = RecordReader.builder(input).build();

      while (reader.hasNext()) {
        var record = reader.next();
        Assertions.assertArrayEquals(record.key(), records1.key());
        Assertions.assertArrayEquals(record.value(), records1.value());
        Assertions.assertEquals(record.topic(), records1.topic());
        Assertions.assertEquals(record.partition(), records1.partition());
        Assertions.assertEquals(record.timestamp(), records1.timestamp());
        Assertions.assertEquals(record.offset(), records1.offset());
      }
      task.close();
    }
  }

  @Test
  void testHdfsSinkTaskIntervalWith2Writers() {
    try (var server = HdfsServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new Exporter.Task();
      var configs =
          Map.of(
              "fs.schema",
              "hdfs",
              "topics",
              topicName,
              "fs.hdfs.hostname",
              String.valueOf(server.hostname()),
              "fs.hdfs.port",
              String.valueOf(server.port()),
              "fs.hdfs.user",
              String.valueOf(server.user()),
              "path",
              "/" + fileSize,
              "size",
              fileSize,
              "tasks.max",
              "1",
              "roll.duration",
              "100ms");

      task.start(configs);

      var record1 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test0".getBytes())
              .partition(0)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build();

      var record2 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test1".getBytes())
              .partition(1)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build();

      var record3 =
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test2".getBytes())
              .partition(0)
              .offset(1)
              .timestamp(System.currentTimeMillis())
              .build();

      task.put(List.of(record1));
      Utils.sleep(Duration.ofMillis(500));

      task.put(List.of(record2));
      Utils.sleep(Duration.ofMillis(1000));

      task.put(List.of(record3));
      Utils.sleep(Duration.ofMillis(1000));

      var fs = FileSystem.of("hdfs", Configuration.of(configs));

      Assertions.assertEquals(
          2, fs.listFolders("/" + String.join("/", fileSize, topicName)).size());

      Assertions.assertEquals(
          2, fs.listFiles("/" + String.join("/", fileSize, topicName, "0")).size());

      List.of(record1, record2, record3)
          .forEach(
              sinkRecord -> {
                var input =
                    fs.read(
                        "/"
                            + String.join(
                                "/",
                                fileSize,
                                topicName,
                                String.valueOf(sinkRecord.partition()),
                                String.valueOf(sinkRecord.offset())));
                var reader = RecordReader.builder(input).build();

                while (reader.hasNext()) {
                  var record = reader.next();
                  Assertions.assertArrayEquals(record.key(), sinkRecord.key());
                  Assertions.assertArrayEquals(record.value(), sinkRecord.value());
                  Assertions.assertEquals(record.topic(), sinkRecord.topic());
                  Assertions.assertEquals(record.partition(), sinkRecord.partition());
                  Assertions.assertEquals(record.timestamp(), sinkRecord.timestamp());
                  Assertions.assertEquals(record.offset(), sinkRecord.offset());
                }
              });
      task.close();
    }
  }
}
