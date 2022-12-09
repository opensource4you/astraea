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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.fs.FileSystem;
import org.astraea.it.FtpServer;
import org.astraea.it.RequireWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FtpConnectorSinkTest extends RequireWorkerCluster {

  @Test
  void testCreteFtpSinkConnector() {

    var topicName = Utils.randomString(10);

    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    Map<String, String> connectorConfigs =
        Map.of(
            "connector.class",
            FtpSinkConnector.class.getName(),
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
    Assertions.assertEquals(FtpSinkConnector.class.getName(), configs.get("connector.class"));
  }

  @Test
  void testFtpSinkTask() {
    try (var server = FtpServer.local()) {
      var fileSize = "500Byte";
      var topicName = Utils.randomString(10);

      var task = new FtpSinkTask();
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

      Collection<SinkRecord> records =
          List.of(
              new SinkRecord(
                  topicName,
                  0,
                  Schema.BYTES_SCHEMA,
                  "test".getBytes(),
                  Schema.BYTES_SCHEMA,
                  "test0".getBytes(),
                  0,
                  System.currentTimeMillis(),
                  TimestampType.CREATE_TIME),
              new SinkRecord(
                  topicName,
                  1,
                  Schema.BYTES_SCHEMA,
                  "test".getBytes(),
                  Schema.BYTES_SCHEMA,
                  "test1".getBytes(),
                  1,
                  System.currentTimeMillis(),
                  TimestampType.CREATE_TIME));

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
                            String.valueOf(sinkRecord.kafkaPartition()),
                            String.valueOf(sinkRecord.kafkaOffset())));
            var reader = RecordReader.builder(input).build();

            while (reader.hasNext()) {
              var record = reader.next();
              Assertions.assertArrayEquals(record.key(), (byte[]) sinkRecord.key());
              Assertions.assertArrayEquals(record.value(), (byte[]) sinkRecord.value());
              Assertions.assertEquals(record.topic(), sinkRecord.topic());
              Assertions.assertEquals(record.partition(), sinkRecord.kafkaPartition());
              Assertions.assertEquals(record.timestamp(), sinkRecord.timestamp());
            }
          });
    }
  }
}
