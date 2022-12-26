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

import java.util.List;
import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.consumer.Record;
import org.astraea.fs.FileSystem;
import org.astraea.it.FtpServer;
import org.astraea.it.RequireWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExporterTest extends RequireWorkerCluster {

  @Test
  void testCreateFtpSinkConnector() {

    var topicName = Utils.randomString(10);

    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
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
              "1");

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
              Assertions.assertArrayEquals(record.key(), (byte[]) sinkRecord.key());
              Assertions.assertArrayEquals(record.value(), (byte[]) sinkRecord.value());
              Assertions.assertEquals(record.topic(), sinkRecord.topic());
              Assertions.assertEquals(record.partition(), sinkRecord.partition());
              Assertions.assertEquals(record.timestamp(), sinkRecord.timestamp());
            }
          });
    }
  }
}
