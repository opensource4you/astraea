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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.consumer.Record;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
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

  static Definition BUFFER_SIZE_KEY =
      Definition.builder()
          .name("writer.buffer.size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .documentation(
              "a value that represents the capacity of a blocking queue from which the writer can take records.")
          .defaultValue("300MB")
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
    String path;
    DataSize size;
    long interval;
    private final Map<String, Boolean> topicWithTarget = new HashMap<>();

    final ConcurrentHashMap<String, TargetStatus> targetForTopicPartition =
        new ConcurrentHashMap<>();

    private final Map<TopicPartition, Long> seekOffset = new HashMap<>();

    //  package public for test
    static class TargetStatus {

      private final Map<String, List<Long>> targets;

      private final Map<String, Boolean> initExclude;

      private Long nextInvalidOffset = 0L;

      TargetStatus() {
        this.targets = new HashMap<>();
        this.targets.put("offset", new ArrayList<>());
        this.targets.put("timestamp", new ArrayList<>());
        this.initExclude = new HashMap<>();
      }

      TargetStatus(Map<String, List<Long>> targets, Map<String, Boolean> initExclude) {

        this.targets = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : targets.entrySet()) {
          String key = entry.getKey();
          List<Long> value = entry.getValue();
          this.targets.put(key, new ArrayList<>(value));
        }
        this.initExclude = new HashMap<>(initExclude);
      }

      private boolean calStatus(boolean initStatus, int index) {
        if (index % 2 == 1) return !initStatus;
        return initStatus;
      }

      /**
       * Inserts a range of values into the target list for a given type. The range starts from the
       * "from" value (inclusive) and ends at the "to" value (inclusive). If "exclude" is true, the
       * range is excluded from the target list. If the target list for the given type is empty, a
       * default value of 0 is added at the beginning, and the exclude flag is set to the opposite
       * of the given "exclude" value.
       *
       * @param type the type of target list to modify
       * @param from the starting value of the range to insert (inclusive)
       * @param to the ending value of the range to insert (inclusive)
       * @param exclude whether to exclude the inserted range from the target list
       */
      void insertRange(String type, Long from, Long to, boolean exclude) {
        to++;
        var target = this.targets.get(type);
        if (target.isEmpty()) {
          target.add(0L);
          this.initExclude.put(type, !exclude);
        }

        int indexBeforeFromShouldBe = 0;
        int indexAfterToShouldBe = target.size();

        for (var i = 1; i < target.size(); i++) {
          if (target.get(i) > from) {
            indexBeforeFromShouldBe = i - 1;
            break;
          }
        }

        for (var i = indexBeforeFromShouldBe + 1; i < target.size(); i++) {
          if (target.get(i) > to) {
            indexAfterToShouldBe = i;
            break;
          }
        }

        var deletedIndexFrom = indexBeforeFromShouldBe + 1;
        var deletedIndexTo = indexAfterToShouldBe;

        if (exclude == calStatus(this.initExclude.get(type), indexAfterToShouldBe)) {
          target.add(indexAfterToShouldBe, to);
        }

        if (from == 0 && from == indexBeforeFromShouldBe) {
          this.initExclude.put(type, exclude);
        } else {
          if (exclude != calStatus(this.initExclude.get(type), indexBeforeFromShouldBe)) {
            target.add(indexBeforeFromShouldBe + 1, from);
            deletedIndexFrom++;
            deletedIndexTo++;
          }
        }

        if (indexAfterToShouldBe - indexBeforeFromShouldBe != 1) {
          target.subList(deletedIndexFrom, deletedIndexTo).clear();
        }
      }

      void updateNextInvalidOffset(Long offset) {
        this.nextInvalidOffset = this.nextInvalidOffset(offset);
      }

      Map<String, List<Long>> targets() {
        return this.targets;
      }

      List<Long> targets(String type) {
        return this.targets.get(type);
      }

      Map<String, Boolean> initExcludes() {
        return this.initExclude;
      }

      Boolean initialExclude(String type) {
        return this.initExclude.get(type);
      }

      Boolean isTargetOffset(Long offset) {
        for (var i = 0; i < targets("offset").size(); i++) {
          if (targets("offset").get(i) > offset) {
            return !calStatus(initialExclude("offset"), i - 1);
          }
        }
        return !calStatus(initialExclude("offset"), targets("offset").size() - 1);
      }

      // Finds the next valid offset given a current offset
      Long nextValidOffset(Long currentOffset) {
        // find the next offset in targets.offset which value greater or equal to currentOffset.
        return targets("offset").stream()
            .filter(offset -> offset > currentOffset)
            .filter(
                offset -> !calStatus(initialExclude("offset"), targets("offset").indexOf(offset)))
            .findFirst()
            .orElse(Long.MAX_VALUE);
      }

      Long nextInvalidOffset(Long currentOffset) {
        return targets("offset").stream()
            .filter(offset -> offset > currentOffset)
            .filter(
                offset -> calStatus(initialExclude("offset"), targets("offset").indexOf(offset)))
            .findFirst()
            .orElse(Long.MAX_VALUE);
      }
    }

    RecordWriter createRecordWriter(TopicPartition tp, long offset) {
      var fileName = String.valueOf(offset);
      return RecordWriter.builder(
              fs.write(
                  String.join("/", path, tp.topic(), String.valueOf(tp.partition()), fileName)))
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

    void updateTargetRangesForTopicPartitions(List<HashMap<String, Object>> targets) {
      targets.forEach(
          target -> {
            var topic = target.get("topic");
            var partition = target.get("partition");

            if (partition == null) {
              partition = "all";
            }

            this.topicWithTarget.put((String) topic, true);

            if (target.get("range") instanceof Map<?, ?>) {
              Map<String, Object> range = (Map<String, Object>) target.get("range");
              var type = (String) range.get("type");
              var from = Long.parseLong((String) range.get("from"));
              var to = Long.parseLong((String) range.get("to"));
              var exclude = (boolean) range.get("exclude");
              // Get the TargetStatus for the topic-partition or create a new one if it doesn't
              // exist
              this.targetForTopicPartition
                  .computeIfAbsent(
                      topic + "-" + partition,
                      tp -> {
                        var base = this.targetForTopicPartition.get(topic + "-all");
                        if (base == null) {
                          return new TargetStatus();
                        }
                        return new TargetStatus(base.targets(), base.initExcludes());
                      })
                  .insertRange(type, from, to, exclude);
              // If the partition is "all", insert the range into all the TargetStatus
              if (partition == "all") {
                this.targetForTopicPartition.forEach(
                    (key, value) -> value.insertRange(type, from, to, exclude));
              }
            }
          });
    }

    @Override
    protected void init(Configuration configuration) {
      this.path = configuration.requireString(PATH_KEY.name());
      this.size =
          DataSize.of(
              configuration.string(SIZE_KEY.name()).orElse(SIZE_KEY.defaultValue().toString()));
      this.interval =
          Utils.toDuration(
                  configuration.string(TIME_KEY.name()).orElse(TIME_KEY.defaultValue().toString()))
              .toMillis();

      this.bufferSize.reset();

      this.bufferSizeLimit =
          DataSize.of(
                  configuration
                      .string(BUFFER_SIZE_KEY.name())
                      .orElse(BUFFER_SIZE_KEY.defaultValue().toString()))
              .bytes();

      List<HashMap<String, Object>> targets =
          JsonConverter.jackson()
              .fromJson(configuration.string("targets").orElse("[]"), new TypeRef<>() {});

      if (targets.size() > 0) {
        updateTargetRangesForTopicPartitions(targets);
      }

      this.fs = FileSystem.of(configuration.requireString(SCHEMA_KEY.name()), configuration);
      this.writerFuture = CompletableFuture.runAsync(createWriter());
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      records.forEach(
          r ->
              Utils.packException(
                  () -> {
                    if (!isValid(r, true)) return;

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
      if (this.seekOffset.size() != 0) {
        this.seekOffset.forEach(
            (tp, offset) -> {
              this.context.offset(
                  new org.apache.kafka.common.TopicPartition(tp.topic(), tp.partition()), offset);
              this.seekOffset.remove(tp);
            });
        this.context.requestCommit();
      }
    }

    boolean isValid(Record<byte[], byte[]> r) {
      return isValid(r, false);
    }

    private boolean isValid(Record<byte[], byte[]> r, boolean contextOperation) {
      if (this.topicWithTarget.get(r.topic()) != null) {
        // this topic has user target, we should check this offset is valid.
        var target = this.targetForTopicPartition.get(r.topic() + "-" + r.partition());
        if (target == null) {
          target = this.targetForTopicPartition.get(r.topic() + "-all");
        }
        if (r.offset() >= target.nextInvalidOffset) {
          // this record is not in the previous valid target range, we should check this offset is
          // valid or not.

          if (target.isTargetOffset(r.offset())) {
            // we are in 1 of the range, we just update the nextInvalidOffset.
            target.updateNextInvalidOffset(r.offset());
          } else {
            // we are not in the valid target rang, we should seek the next valid offset.
            var nextValidOffset = target.nextValidOffset(r.offset());
            if (nextValidOffset != Long.MAX_VALUE) {
              // todo have to check the max offset of this topic partition before we reset
              // the offset.
              if (contextOperation) this.seekOffset.put(r.topicPartition(), nextValidOffset);
            } else {
              // subsequent offsets are not within the user-specified range, so ew should stop
              // consuming to avoid consuming more unnecessary data.
              this.seekOffset.remove(r.topicPartition());
              if (contextOperation) this.context.pause();
            }
            return false;
          }
        }
      }
      var seekOffset = this.seekOffset.get(r.topicPartition());
      if (seekOffset != null) {
        this.seekOffset.put(
            r.topicPartition(), Math.max(r.offset() + 1, this.seekOffset.get(r.topicPartition())));
      }
      return true;
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
