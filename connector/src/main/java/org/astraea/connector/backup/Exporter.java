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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
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
          .documentation("decide which file system to use, such as FTP, HDFS.")
          .required()
          .build();
  static Definition HOSTNAME_KEY =
      Definition.builder()
          .name("fs.<schema>.hostname")
          .type(Definition.Type.STRING)
          .documentation("the host name of the <schema> server used.")
          .build();
  static Definition PORT_KEY =
      Definition.builder()
          .name("fs.<schema>.port")
          .type(Definition.Type.STRING)
          .documentation("the port of the <schema> server used.")
          .build();
  static Definition USER_KEY =
      Definition.builder()
          .name("fs.<schema>.user")
          .type(Definition.Type.STRING)
          .documentation("the user name required to login to the <schema> server.")
          .build();
  static Definition PASSWORD_KEY =
      Definition.builder()
          .name("fs.<schema>.password")
          .type(Definition.Type.PASSWORD)
          .documentation("the password required to login to the <schema> server.")
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

  static Definition OVERRIDE_KEY =
      Definition.builder()
          .name("fs.<schema>.override.<property_name>")
          .type(Definition.Type.STRING)
          .documentation("a value that needs to be overridden in the file system.")
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
    return List.of(
        SCHEMA_KEY,
        HOSTNAME_KEY,
        PORT_KEY,
        USER_KEY,
        PASSWORD_KEY,
        PATH_KEY,
        SIZE_KEY,
        OVERRIDE_KEY);
  }

  public static class Task extends SinkTask {

    private CompletableFuture<Void> writerFuture;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final BlockingQueue<Record<byte[], byte[]>> recordsQueue = new LinkedBlockingQueue<>();

    static Runnable createWriter(
        FileSystem fs,
        String path,
        String topicName,
        long interval,
        DataSize size,
        Supplier<Boolean> closed,
        Supplier<Record<byte[], byte[]>> recordsQueue) {
      return () -> {
        var writers = new HashMap<TopicPartition, RecordWriter>();
        var longestWriteTime = System.currentTimeMillis();

        try {
          while (!closed.get()) {
            var record = recordsQueue.get();
            var currentTime = System.currentTimeMillis();

            if (currentTime - longestWriteTime > interval) {
              longestWriteTime = currentTime;
              var itr = writers.values().iterator();
              while (itr.hasNext()) {
                var writer = itr.next();
                if (currentTime - writer.latestAppendTimestamp() > interval) {
                  writer.close();
                  itr.remove();
                } else {
                  longestWriteTime = Math.min(longestWriteTime, writer.latestAppendTimestamp());
                }
              }
            }

            if (record != null) {
              var writer =
                  writers.computeIfAbsent(
                      record.topicPartition(),
                      ignored -> {
                        var fileName = String.valueOf(record.offset());
                        return RecordWriter.builder(
                                fs.write(
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
          }
        } finally {
          writers.forEach((tp, writer) -> writer.close());
        }
      };
    }

    @Override
    protected void init(Configuration configuration) {
      var topicName = configuration.requireString(TOPICS_KEY);
      var path = configuration.requireString(PATH_KEY.name());
      var size =
          DataSize.of(
              configuration.string(SIZE_KEY.name()).orElse(SIZE_KEY.defaultValue().toString()));
      var interval =
          Utils.toDuration(
                  configuration.string(TIME_KEY.name()).orElse(TIME_KEY.defaultValue().toString()))
              .toMillis();

      var fs = FileSystem.of(configuration.requireString(SCHEMA_KEY.name()), configuration);
      this.writerFuture =
          CompletableFuture.runAsync(
              createWriter(
                  fs,
                  path,
                  topicName,
                  interval,
                  size,
                  this.closed::get,
                  () ->
                      Utils.packException(
                          () ->
                              this.recordsQueue.poll(
                                  Math.min(interval, 1000), TimeUnit.MILLISECONDS))));
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      recordsQueue.addAll(records);
    }

    @Override
    protected void close() {
      this.closed.set(true);
      Utils.packException(() -> writerFuture.toCompletableFuture().get(10, TimeUnit.SECONDS));
    }

    boolean isWriterDone() {
      return writerFuture.isDone();
    }
  }
}
