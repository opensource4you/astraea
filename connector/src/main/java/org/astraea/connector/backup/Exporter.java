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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
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
          .documentation("decide which file system to use, such as FTP.")
          .required()
          .build();
  static Definition HOSTNAME_KEY =
      Definition.builder()
          .name("fs.ftp.hostname")
          .type(Definition.Type.STRING)
          .documentation("the host name of the ftp server used.")
          .build();
  static Definition PORT_KEY =
      Definition.builder()
          .name("fs.ftp.port")
          .type(Definition.Type.STRING)
          .documentation("the port of the ftp server used.")
          .build();
  static Definition USER_KEY =
      Definition.builder()
          .name("fs.ftp.user")
          .type(Definition.Type.STRING)
          .documentation("the user name required to login to the FTP server.")
          .build();
  static Definition PASSWORD_KEY =
      Definition.builder()
          .name("fs.ftp.password")
          .type(Definition.Type.PASSWORD)
          .documentation("the password required to login to the ftp server.")
          .build();
  static Definition PATH_KEY =
      Definition.builder()
          .name("path")
          .type(Definition.Type.STRING)
          .documentation("the path required for file storage.")
          .required()
          .build();
  static Definition SIZE_KEY =
      Definition.builder()
          .name("size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue("100MB")
          .documentation("is the maximum number of the size will be included in each file.")
          .build();

  static Definition TIME_KEY =
      Definition.builder()
          .name("time")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> Utils.toDuration(obj.toString()))
          .defaultValue("3s")
          .documentation("is the interval to split the file.")
          .build();
  private Configuration configs;

  @Override
  protected void init(Configuration configuration) {
    this.configs = configuration;
  }

  @Override
  protected Class<? extends SinkTask> task() {
    return Task.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    return IntStream.range(0, maxTasks).mapToObj(ignored -> configs).collect(Collectors.toList());
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(SCHEMA_KEY, HOSTNAME_KEY, PORT_KEY, USER_KEY, PASSWORD_KEY, PATH_KEY, SIZE_KEY);
  }

  public static class Task extends SinkTask {
    private FileSystem ftpClient;
    private String topicName;
    private String path;
    private DataSize size;

    private String time;

    private Writer writer;
    private ExecutorService executor = Executors.newFixedThreadPool(1);
    private final List<Record> recordList = Collections.synchronizedList(new ArrayList<>());

    class Writer implements Runnable {
      private final FileSystem client;

      private boolean running = true;

      private Long time;

      private ConcurrentHashMap<TopicPartition, RecordWriter> writers = new ConcurrentHashMap<>();

      private void writeRecord(Record<byte[], byte[]> record) {
        var writer =
            this.writers.computeIfAbsent(
                record.topicPartition(),
                ignored -> {
                  var fileName = String.valueOf(record.offset());
                  return RecordWriter.builder(
                          this.client.write(
                              String.join(
                                  "/",
                                  path,
                                  topicName,
                                  String.valueOf(record.partition()),
                                  fileName)))
                      .build();
                });

        writer.append(record);

        if (writer.size().greaterThan(size)) {
          this.writers.remove(record.topicPartition()).close();
        }
      }

      private void flushWriters() {
        var currentTime = System.currentTimeMillis();
        this.writers.forEach(
            (tp, writer) -> {
              if (currentTime - writer.time() > this.time) {
                this.writers.remove(tp).close();
              }
            });
      }

      public Writer(FileSystem client, Long time) {
        this.client = client;
        this.time = time;
      }

      @Override
      public void run() {
        while (running) {
          synchronized (recordList) {
            if (!recordList.isEmpty()) {
              recordList.forEach(this::writeRecord);
              recordList.clear();
            }
          }
          flushWriters();
        }
      }

      public void stop() {
        running = false;
        synchronized (recordList) {
          if (!recordList.isEmpty()) {
            recordList.forEach(this::writeRecord);
            recordList.clear();
          }
        }
        writers.forEach((tp, writer) -> writer.close());
      }
    }

    @Override
    protected void init(Configuration configuration) {
      this.ftpClient = FileSystem.of(configuration.requireString(SCHEMA_KEY.name()), configuration);
      this.topicName = configuration.requireString(TOPICS_KEY);
      this.path = configuration.requireString(PATH_KEY.name());
      this.size =
          DataSize.of(
              configuration.string(SIZE_KEY.name()).orElse(SIZE_KEY.defaultValue().toString()));
      this.time = configuration.string(TIME_KEY.name()).orElse(TIME_KEY.defaultValue().toString());
      writer = new Writer(this.ftpClient, Utils.toDuration(this.time).toMillis());
      executor.execute(writer);
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      synchronized (recordList) {
        recordList.addAll(records);
      }
    }

    @Override
    protected void close() throws InterruptedException {
      writer.stop();
      executor.shutdown();
      this.ftpClient.close();
    }
  }
}
