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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.connector.Config;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.Value;
import org.astraea.common.consumer.Record;
import org.astraea.connector.MetadataStorage;
import org.astraea.fs.FileSystem;
import org.astraea.it.FtpServer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ImporterTest {

  private static final Service SERVICE =
      Service.builder().numberOfWorkers(1).numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testRequireConfigs() {
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var validation =
        client.validate(Importer.class.getName(), Map.of("name", "b")).toCompletableFuture().join();
    Assertions.assertNotEquals(0, validation.errorCount());

    var failed =
        validation.configs().stream()
            .map(Config::value)
            .filter(v -> !v.errors().isEmpty())
            .collect(Collectors.toMap(Value::name, Function.identity()));
    Assertions.assertEquals(
        Set.of(Importer.SCHEMA_KEY.name(), Importer.PATH_KEY.name()), failed.keySet());
  }

  @Test
  void testCreateFtpSourceConnector() {
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    Map<String, String> connectorConfigs =
        Map.of(
            "fs.schema",
            "ftp",
            "connector.class",
            Importer.class.getName(),
            "tasks.max",
            "2",
            "fs.ftp.hostname",
            "127.0.0.1",
            "fs.ftp.port",
            "21",
            "fs.ftp.user",
            "user",
            "fs.ftp.password",
            "password",
            "path",
            "/source",
            "clean.source",
            "archive",
            "archive.dir",
            "/archive");
    var createdConnectorInfo =
        connectorClient.createConnector("FtpSource", connectorConfigs).toCompletableFuture().join();

    var configs = createdConnectorInfo.config();

    Assertions.assertEquals("FtpSource", createdConnectorInfo.name());
    Assertions.assertEquals(Importer.class.getName(), configs.get("connector.class"));
    Assertions.assertEquals("2", configs.get("tasks.max"));
  }

  @Test
  void testFtpSourceTask() {
    try (var server = FtpServer.local()) {
      var topicName = Utils.randomString(10);
      var task = new Importer.Task();
      var configs =
          Map.of(
              "fs.schema",
              "ftp",
              "connector.class",
              Importer.class.getName(),
              "tasks.count",
              "1",
              "path",
              "/source",
              "fs.ftp.hostname",
              String.valueOf(server.hostname()),
              "fs.ftp.port",
              String.valueOf(server.port()),
              "fs.ftp.user",
              String.valueOf(server.user()),
              "fs.ftp.password",
              String.valueOf(server.password()),
              "clean.source",
              "off",
              "file.set",
              "0");

      var fs = FileSystem.of("ftp", new Configuration(configs));

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

      var os = fs.write("/source/topic/0/0");
      var writer = RecordWriter.builder(os).build();
      records.forEach(writer::append);
      writer.close();

      task.init(new Configuration(configs), MetadataStorage.EMPTY);
      var returnRecords = new ArrayList<>(task.take());

      for (int i = 0; i < records.size(); i++) {
        Assertions.assertEquals(records.get(i).topic(), returnRecords.get(i).topic());
        Assertions.assertArrayEquals(records.get(i).key(), returnRecords.get(i).key());
        Assertions.assertArrayEquals(records.get(i).value(), returnRecords.get(i).value());
        Assertions.assertEquals(records.get(i).partition(), returnRecords.get(i).partition().get());
        Assertions.assertEquals(records.get(i).timestamp(), returnRecords.get(i).timestamp().get());
      }
    }
  }
}
