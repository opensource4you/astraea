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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.connector.Config;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.connector.Value;
import org.astraea.common.consumer.Record;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
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

  @Test
  void testGetRecordsFromBuffer() {
    var topicName = Utils.randomString(10);
    var records =
        List.of(
            Record.builder()
                .topic(topicName)
                .key("test".getBytes())
                .value("test0".getBytes())
                .partition(0)
                .offset(0)
                .timestamp(System.currentTimeMillis())
                .build(),
            Record.builder()
                .topic(topicName)
                .key("test".getBytes())
                .value("test1".getBytes())
                .partition(1)
                .offset(0)
                .timestamp(System.currentTimeMillis())
                .build());

    var task = new Exporter.Task();
    task.bufferSize.reset();

    records.forEach(
        record -> {
          task.recordsQueue.offer(record);
          task.bufferSize.add(record.serializedKeySize() + record.serializedValueSize());
        });

    var list = task.recordsFromBuffer();

    Assertions.assertEquals(records, list);
  }

  /** The purpose of this test is also to remove old writers */
  @Test
  void testCreateRecordWriter() {
    try (var server = HdfsServer.local()) {
      var path = "/test";
      var topicName = Utils.randomString(10);
      var tp = TopicPartition.of(topicName, 0);
      long offset = 123;
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
              path,
              "size",
              "100MB",
              "tasks.max",
              "1",
              "roll.duration",
              "300ms");

      var writers = new HashMap<TopicPartition, RecordWriter>();

      var task = new Exporter.Task();
      task.fs = FileSystem.of("hdfs", Configuration.of(configs));
      task.interval = 1000;

      RecordWriter recordWriter = task.createRecordWriter(tp, offset);

      Assertions.assertNotNull(recordWriter);

      writers.put(tp, recordWriter);

      recordWriter.append(
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test0".getBytes())
              .partition(0)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build());

      task.removeOldWriters(writers);

      // the writer should not be closed before sleep.
      Assertions.assertNotEquals(0, writers.size());

      Utils.sleep(Duration.ofMillis(1500));

      task.removeOldWriters(writers);

      Assertions.assertEquals(0, writers.size());
    }
  }

  @Test
  void testWriteRecords() {
    try (var server = HdfsServer.local()) {
      var topicName = Utils.randomString(10);
      var path = "/test";
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
              path,
              "size",
              "100MB",
              "tasks.max",
              "1",
              "roll.duration",
              "300ms");

      var writers = new HashMap<TopicPartition, RecordWriter>();

      var task = new Exporter.Task();
      task.fs = FileSystem.of("hdfs", Configuration.of(configs));
      task.size = DataSize.of("100MB");
      task.bufferSize.reset();
      task.recordsQueue.add(
          Record.builder()
              .topic(topicName)
              .key("test".getBytes())
              .value("test0".getBytes())
              .partition(0)
              .offset(0)
              .timestamp(System.currentTimeMillis())
              .build());

      Assertions.assertNotEquals(0, task.recordsQueue.size());

      task.writeRecords(writers);

      Assertions.assertEquals(0, task.recordsQueue.size());
    }
  }

  @Test
  void testInsertRange() {

    var targetStatus1 = new Exporter.Task.TargetStatus();
    targetStatus1.insertRange("offset", 0L, 100L, true);
    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 101L)), targetStatus1.targets("offset"));
    Assertions.assertTrue(targetStatus1.initialExclude("offset"));

    targetStatus1.insertRange("offset", 0L, 100L, false);
    Assertions.assertEquals(new ArrayList<>(List.of(0L)), targetStatus1.targets("offset"));
    Assertions.assertFalse(targetStatus1.initialExclude("offset"));

    targetStatus1.insertRange("offset", 5L, 10L, false);
    Assertions.assertEquals(new ArrayList<>(List.of(0L)), targetStatus1.targets("offset"));
    Assertions.assertFalse(targetStatus1.initialExclude("offset"));

    targetStatus1.insertRange("offset", 5L, 10L, true);
    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 5L, 11L)), targetStatus1.targets("offset"));
    Assertions.assertFalse(targetStatus1.initialExclude("offset"));

    var targetStatus2 = new Exporter.Task.TargetStatus(targetStatus1);
    var targetStatus3 = new Exporter.Task.TargetStatus(targetStatus1);

    targetStatus1.insertRange("offset", 2L, 7L, true);
    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 2L, 11L)), targetStatus1.targets("offset"));
    Assertions.assertFalse(targetStatus1.initialExclude("offset"));

    targetStatus2.insertRange("offset", 7L, 12L, true);
    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 5L, 13L)), targetStatus2.targets("offset"));
    Assertions.assertFalse(targetStatus2.initialExclude("offset"));

    targetStatus3.insertRange("offset", 7L, 8L, false);
    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 5L, 7L, 9L, 11L)), targetStatus3.targets("offset"));
    Assertions.assertFalse(targetStatus3.initialExclude("offset"));
  }

  @Test
  void testMapTpToTarget() {
    var configs = new HashMap<String, String>();

    var rangesInConfigs1 =
        List.of(
            Map.of(
                "topic",
                "test",
                "range",
                Map.of("from", "0", "to", "100", "exclude", false, "type", "offset")),
            Map.of(
                "topic",
                "test",
                "partition",
                "0",
                "range",
                Map.of("from", "10", "to", "50", "exclude", true, "type", "offset")),
            Map.of(
                "topic",
                "test",
                "partition",
                "1",
                "range",
                Map.of("from", "20", "to", "50", "exclude", true, "type", "offset")),
            Map.of(
                "topic",
                "test",
                "range",
                Map.of("from", "30", "to", "50", "exclude", false, "type", "offset")),
            Map.of("topic", "test1", "partition", "0"));

    configs.put("targets", JsonConverter.jackson().toJson(rangesInConfigs1));

    var configuration = Configuration.of(configs);

    var task = new Exporter.Task();

    List<HashMap<String, Object>> targets =
        JsonConverter.jackson()
            .fromJson(configuration.string("targets").orElse("[]"), new TypeRef<>() {});

    task.updateTargetRangesForTopicPartitions(targets);

    var target0 = task.targetForTopicPartition.get("test-0");
    var target1 = task.targetForTopicPartition.get("test-1");
    var targetAll = task.targetForTopicPartition.get("test-all");
    var targetForTest1 = task.targetForTopicPartition.get("test1-0");

    Assertions.assertFalse(target0.initialExclude("offset"));
    Assertions.assertFalse(target1.initialExclude("offset"));
    Assertions.assertFalse(targetAll.initialExclude("offset"));

    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 10L, 30L, 101L)), target0.targets("offset"));
    Assertions.assertEquals(
        new ArrayList<>(Arrays.asList(0L, 20L, 30L, 101L)), target1.targets("offset"));
    Assertions.assertEquals(new ArrayList<>(Arrays.asList(0L, 101L)), targetAll.targets("offset"));
    Assertions.assertEquals(new ArrayList<>(List.of(0L)), targetForTest1.targets("offset"));

    Assertions.assertTrue(target0.isTargetOffset(0L));
    Assertions.assertTrue(target0.isTargetOffset(9L));
    Assertions.assertTrue(target0.isTargetOffset(100L));
    Assertions.assertFalse(target0.isTargetOffset(10L));
    Assertions.assertFalse(target0.isTargetOffset(29L));
    Assertions.assertFalse(target0.isTargetOffset(101L));

    Assertions.assertEquals(10L, target0.nextInvalidOffset(9L).orElseThrow());
    Assertions.assertEquals(101L, target0.nextInvalidOffset(10L).orElseThrow());
    Assertions.assertEquals(30L, target0.nextValidOffset(0L).orElseThrow());
    Assertions.assertEquals(30L, target0.nextValidOffset(10L).orElseThrow());
  }

  @Test
  void testIsValid() {
    var configs = new HashMap<String, String>();

    var rangesInConfigs1 =
        List.of(
            Map.of(
                "topic",
                "test",
                "range",
                Map.of("from", "0", "to", "20", "exclude", false, "type", "offset")),
            Map.of(
                "topic",
                "test",
                "partition",
                "0",
                "range",
                Map.of("from", "10", "to", "18", "exclude", true, "type", "offset")),
            Map.of(
                "topic",
                "test",
                "range",
                Map.of("from", "15", "to", "18", "exclude", false, "type", "offset")));

    configs.put("targets", JsonConverter.jackson().toJson(rangesInConfigs1));

    var configuration = Configuration.of(configs);
    var task = new Exporter.Task();
    List<HashMap<String, Object>> targets =
        JsonConverter.jackson()
            .fromJson(configuration.string("targets").orElse("[]"), new TypeRef<>() {});

    task.updateTargetRangesForTopicPartitions(targets);

    List<Record<byte[], byte[]>> records = new ArrayList<>();

    for (var i = 0; i <= 21; i++) {
      records.add(
          Record.builder()
              .topic("test")
              .key("test".getBytes())
              .value("test".getBytes())
              .partition(0)
              .offset(i)
              .timestamp(System.currentTimeMillis())
              .build());
    }

    records.stream()
        .filter(record -> record.offset() < 10)
        .forEach(record -> Assertions.assertTrue(task.isValid(record)));

    records.stream()
        .filter(record -> record.offset() >= 10)
        .filter(record -> record.offset() < 15)
        .forEach(record -> Assertions.assertFalse(task.isValid(record)));

    records.stream()
        .filter(record -> record.offset() >= 15)
        .filter(record -> record.offset() <= 20)
        .forEach(record -> Assertions.assertTrue(task.isValid(record)));

    Assertions.assertFalse(task.isValid(records.get(21)));
  }
}
