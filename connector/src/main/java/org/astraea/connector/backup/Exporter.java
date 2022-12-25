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
import java.util.HashMap;
import java.util.List;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.consumer.Record;
import org.astraea.connector.Definition;
import org.astraea.connector.SinkConnector;
import org.astraea.connector.SinkTask;
import org.astraea.fs.FileSystem;

public class Exporter extends SinkConnector {

  static Definition SCHEMA_KEY =
      Definition.builder()
          .name("fs.schema")
          .type(Definition.Type.STRING)
          .documentation("determine which file system needs to use.")
          .build();
  static Definition HOSTNAME_KEY =
      Definition.builder()
          .name("fs.ftp.hostname")
          .type(Definition.Type.STRING)
          .documentation("the host name of the ftp server.")
          .build();
  static Definition PORT_KEY =
      Definition.builder()
          .name("fs.ftp.port")
          .type(Definition.Type.STRING)
          .documentation("the port of the ftp server.")
          .build();
  static Definition USER_KEY =
      Definition.builder()
          .name("fs.ftp.user")
          .type(Definition.Type.STRING)
          .documentation("the user to login the ftp server.")
          .build();
  static Definition PASSWORD_KEY =
      Definition.builder()
          .name("fs.ftp.password")
          .type(Definition.Type.STRING)
          .documentation("the password to login the ftp server.")
          .build();
  static Definition PATH_KEY =
      Definition.builder()
          .name("path")
          .type(Definition.Type.STRING)
          .documentation("the path where the sink files to store.")
          .build();
  static Definition TOPIC_KEY =
      Definition.builder()
          .name("topic")
          .type(Definition.Type.STRING)
          .documentation("the topic names which want to sink to file.")
          .build();
  static Definition SIZE_KEY =
      Definition.builder()
          .name("size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue("100MB")
          .documentation("the number of each file limit size.")
          .build();
  static Definition TIME_KEY =
      Definition.builder()
          .name("time")
          .type(Definition.Type.STRING)
          .defaultValue("3s")
          .documentation("the number of each file limit time to save the file.")
          .build();
  private Configuration cons;

  @Override
  protected void init(Configuration configuration) {
    this.cons = configuration;
    configuration.requireString("topics");
    configuration.requireString("path");
    configuration.requireString("size");
  }

  @Override
  protected Class<? extends SinkTask> task() {
    return Task.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    List<Configuration> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(cons);
    }
    return configs;
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(
        SCHEMA_KEY,
        HOSTNAME_KEY,
        PORT_KEY,
        USER_KEY,
        PASSWORD_KEY,
        PATH_KEY,
        TOPIC_KEY,
        SIZE_KEY,
        TIME_KEY);
  }

  public static class Task extends SinkTask {
    private FileSystem ftpClient;
    private String topicName;
    private String path;
    private String size;

    @Override
    protected void init(Configuration configuration) {
      this.ftpClient = FileSystem.of("ftp", configuration);
      this.topicName = configuration.requireString("topics");
      this.path = configuration.requireString("path");
      this.size = configuration.requireString("size");
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      var writers = new HashMap<TopicPartition, RecordWriter>();
      for (var record : records) {
        var writer =
            writers.computeIfAbsent(
                record.topicPartition(),
                ignored -> {
                  var fileName = String.valueOf(record.offset());
                  return RecordWriter.builder(
                          ftpClient.write(
                              String.join(
                                  "/",
                                  this.path,
                                  this.topicName,
                                  String.valueOf(record.partition()),
                                  fileName)))
                      .build();
                });
        writer.append(record);

        if (writer.size().greaterThan(DataSize.of(this.size))) {
          writers.remove(record.topicPartition()).close();
        }
      }

      writers.forEach((tp, writer) -> writer.close());
    }

    @Override
    protected void close() {
      this.ftpClient.close();
    }
  }
}
