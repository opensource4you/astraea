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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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

  static DataSize SIZE_DEFAULT = DataSize.MB.of(100);
  static Definition SIZE_KEY =
      Definition.builder()
          .name("size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue(SIZE_DEFAULT.toString())
          .documentation("is the maximum number of the size will be included in each file.")
          .build();

  static Duration TIME_DEFAULT = Duration.ofSeconds(3);

  static Definition TIME_KEY =
      Definition.builder()
          .name("roll.duration")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> Utils.toDuration(obj.toString()))
          .defaultValue(TIME_DEFAULT.toSeconds() + "s")
          .documentation("the maximum time before a new archive file is rolling out.")
          .build();

  static Definition OVERRIDE_KEY =
      Definition.builder()
          .name("fs.<schema>.override.<property_name>")
          .type(Definition.Type.STRING)
          .documentation("a value that needs to be overridden in the file system.")
          .build();

  static DataSize BUFFER_SIZE_DEFAULT = DataSize.MB.of(300);

  static Definition BUFFER_SIZE_KEY =
      Definition.builder()
          .name("writer.buffer.size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .documentation(
              "a value that represents the capacity of a blocking queue from which the writer can take records.")
          .defaultValue(BUFFER_SIZE_DEFAULT.toString())
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
    return IntStream.range(0, maxTasks).mapToObj(ignored -> configs).toList();
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
        OVERRIDE_KEY,
        BUFFER_SIZE_KEY);
  }

  public static class Task extends SinkTask {

    private CompletableFuture<Void> writerFuture;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    final BlockingQueue<Record<byte[], byte[]>> recordsQueue = new LinkedBlockingQueue<>();

    final LongAdder bufferSize = new LongAdder();

    private final Object putLock = new Object();

    private long bufferSizeLimit;

    FileSystem fs;
    String topicName;
    String path;
    DataSize size;
    long interval;

    RecordWriter createRecordWriter(TopicPartition tp, long offset) {
      var fileName = String.valueOf(offset);
      return RecordWriter.builder(
              fs.write(String.join("/", path, topicName, String.valueOf(tp.partition()), fileName)))
          .build();
    }

    /**
     * Remove writers that have not appended any records in the past <code>interval</code>
     * milliseconds.
     *
     * @param writers a map of <code>TopicPartition</code> to <code>RecordWriter</code> objects
     */
    void removeOldWriters(HashMap<TopicPartition, RecordWriter> writers) {
      var itr = writers.values().iterator();
      var currentTime = System.currentTimeMillis();
      while (itr.hasNext()) {
        var writer = itr.next();
        if (currentTime - writer.latestAppendTimestamp() > interval) {
          writer.close();
          itr.remove();
        }
      }
    }

    /**
     * Writes a list {@link Record} objects to the specified {@link RecordWriter} objects. If a
     * writer for the specified {@link TopicPartition} does not exist, it is created using the given
     * {@link FileSystem}, path. topic name, partition, and offset.
     *
     * @param writers a {@link HashMap} that maps a {@link TopicPartition} to a {@link RecordWriter}
     *     object
     */
    void writeRecords(HashMap<TopicPartition, RecordWriter> writers) {

      var records = recordsFromBuffer();

      removeOldWriters(writers);

      records.forEach(
          record -> {
            var writer =
                writers.computeIfAbsent(
                    record.topicPartition(), tp -> createRecordWriter(tp, record.offset()));
            writer.append(record);
            if (writer.size().greaterThan(size)) {
              writers.remove(record.topicPartition()).close();
            }
          });
    }

    Runnable createWriter() {
      return () -> {
        var writers = new HashMap<TopicPartition, RecordWriter>();

        try {
          while (!closed.get()) {
            writeRecords(writers);
          }
        } finally {
          writers.forEach((tp, writer) -> writer.close());
        }
      };
    }

    /**
     * Retrieves a list of records from the buffer and empties the records queue.
     *
     * @return a {@link List} of records retrieved from the buffer
     */
    List<Record<byte[], byte[]>> recordsFromBuffer() {
      var list = new ArrayList<Record<byte[], byte[]>>(recordsQueue.size());
      recordsQueue.drainTo(list);
      if (list.size() > 0) {
        var drainedSize =
            list.stream()
                .mapToInt(record -> record.serializedKeySize() + record.serializedValueSize())
                .sum();
        bufferSize.add(-drainedSize);
        synchronized (putLock) {
          putLock.notify();
        }
      }
      return list;
    }

    @Override
    protected void init(Configuration configuration) {
      this.topicName = configuration.requireString(TOPICS_KEY);
      this.path = configuration.requireString(PATH_KEY.name());
      this.size = configuration.string(SIZE_KEY.name()).map(DataSize::of).orElse(SIZE_DEFAULT);
      this.interval =
          configuration
              .string(TIME_KEY.name())
              .map(Utils::toDuration)
              .orElse(TIME_DEFAULT)
              .toMillis();
      this.bufferSize.reset();
      this.bufferSizeLimit =
          configuration
              .string(BUFFER_SIZE_KEY.name())
              .map(DataSize::of)
              .orElse(BUFFER_SIZE_DEFAULT)
              .bytes();
      this.fs = FileSystem.of(configuration.requireString(SCHEMA_KEY.name()), configuration);
      this.writerFuture = CompletableFuture.runAsync(createWriter());
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      records.forEach(
          r ->
              Utils.packException(
                  () -> {
                    int recordLength =
                        Stream.of(r.key(), r.value())
                            .filter(Objects::nonNull)
                            .map(i -> i.length)
                            .reduce(0, Integer::sum);

                    synchronized (putLock) {
                      while (this.bufferSize.sum() + recordLength >= this.bufferSizeLimit) {
                        putLock.wait();
                      }
                    }
                    recordsQueue.put(r);

                    this.bufferSize.add(recordLength);
                  }));
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
