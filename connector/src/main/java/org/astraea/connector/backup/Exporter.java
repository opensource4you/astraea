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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
          .name("roll.duration")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> Utils.toDuration(obj.toString()))
          .defaultValue("3s")
          .documentation("the maximum time before a new archive file is rolling out.")
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

    private CompletableFuture<Void> writerFuture;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Duration interval;

    private final ConcurrentHashMap<TopicPartition, RecordWriter> writers =
        new ConcurrentHashMap<>();

    private final BlockingQueue<Record<byte[], byte[]>> recordsQueue = new LinkedBlockingQueue<>();

    @Override
    protected void init(Configuration configuration)
        throws ExecutionException, InterruptedException {
      this.ftpClient = FileSystem.of(configuration.requireString(SCHEMA_KEY.name()), configuration);
      this.topicName = configuration.requireString(TOPICS_KEY);
      this.path = configuration.requireString(PATH_KEY.name());
      this.size =
          DataSize.of(
              configuration.string(SIZE_KEY.name()).orElse(SIZE_KEY.defaultValue().toString()));
      this.interval =
          Utils.toDuration(
              configuration.string(TIME_KEY.name()).orElse(TIME_KEY.defaultValue().toString()));

      this.writerFuture =
          CompletableFuture.runAsync(
              () -> {
                var intervalTimeInMillis = interval.toMillis();
                long sleepTime = interval.toMillis();
                if (sleepTime > 1000) sleepTime = 1000L;
                try {
                  while (!closed.get()) {
                    var record = recordsQueue.poll(sleepTime, TimeUnit.MILLISECONDS);
                    var currentTime = System.currentTimeMillis();
                    writers.forEach(
                        (tp, recordWriter) -> {
                          if (currentTime - recordWriter.time() > intervalTimeInMillis) {
                            writers.remove(tp).close();
                          }
                        });
                    if (record == null) continue;
                    var writer =
                        writers.computeIfAbsent(
                            record.topicPartition(),
                            ignored -> {
                              var fileName = String.valueOf(record.offset());
                              return RecordWriter.builder(
                                      ftpClient.write(
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
                      writers.remove(record.topicPartition()).close();
                    }
                  }
                } catch (InterruptedException ignored) {
                  // swallow
                } finally {
                  writers.forEach((tp, writer) -> writer.close());
                  ftpClient.close();
                }
              });
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      records.forEach(
          record -> {
            try {
              recordsQueue.put(record);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    }

    @Override
    protected void close() throws InterruptedException, ExecutionException {
      this.closed.set(true);
      writerFuture.toCompletableFuture().get();
      if (!this.writerFuture.isDone()) {
        throw new IllegalStateException();
      }
    }
  }
}
