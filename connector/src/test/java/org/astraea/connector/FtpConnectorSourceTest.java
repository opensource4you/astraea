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
package org.astraea.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.consumer.Record;
import org.astraea.fs.FileSystem;
import org.astraea.it.FtpServer;
import org.astraea.it.RequireWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FtpConnectorSourceTest extends RequireWorkerCluster {
  @Test
  void testCreateFtpSourceConnector() {
    var topicName = Utils.randomString(10);

    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    Map<String, String> connectorConfigs =
        Map.of(
            "connector.class",
            FtpSourceConnector.class.getName(),
            "task.max",
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
            "input",
            "/source",
            "clean.source",
            "true",
            "archive.dir",
            "/archive");
    var createdConnectorInfo =
        connectorClient.createConnector("FTPSource", connectorConfigs).toCompletableFuture().join();

    var configs = createdConnectorInfo.config();

    Assertions.assertEquals("FTPSource", createdConnectorInfo.name());
    Assertions.assertEquals(FtpSourceConnector.class.getName(), configs.get("connector.class"));
    Assertions.assertEquals("2", configs.get("task.max"));
  }

  @Test
  void testFtpSourceTask() {
    try (var server = FtpServer.local()) {
      var topicName = Utils.randomString(10);
      var task = new FtpSourceTask();
      var configs =
          Map.of(
              "task.max",
              "1",
              "connector.class",
              "FTPSourceConnector",
              "topic",
              topicName,
              "input",
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
              "false",
              "archive.dir",
              "/archive");

      var fs = FileSystem.of("ftp", Configuration.of(configs));

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

      var os = fs.write("/source/file");
      var writer = RecordWriter.builder(os).build();
      records.forEach(writer::append);
      writer.close();
      os.close();

      task.init(Configuration.of(configs));
      var returnRecords = new ArrayList<>(task.take());

      for (int i = 0; i < records.size(); i++) {
        Assertions.assertEquals(records.get(i).topic(), returnRecords.get(i).topic());
        Assertions.assertArrayEquals(records.get(i).key(), returnRecords.get(i).key());
        Assertions.assertArrayEquals(records.get(i).value(), returnRecords.get(i).value());
        Assertions.assertEquals(records.get(i).partition(), returnRecords.get(i).partition());
        Assertions.assertEquals(records.get(i).timestamp(), returnRecords.get(i).timestamp());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
