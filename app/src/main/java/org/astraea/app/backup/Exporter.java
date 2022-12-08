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
package org.astraea.app.backup;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import org.astraea.app.argument.DataSizeField;
import org.astraea.app.argument.PathField;
import org.astraea.app.argument.StringSetField;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.IteratorLimit;
import org.astraea.fs.FileSystem;

public class Exporter {

  public static void main(String[] args) {
    var arg = Argument.parse(new Argument(), args);
    System.out.println("prepare to export data from " + arg.topics);
    var result = execute(arg);
    System.out.println(result);
  }

  public static Map<TopicPartition, Stat> execute(Argument argument) {
    if (!argument.output.toFile().isDirectory())
      throw new IllegalArgumentException("--output must be a existent folder");
    try (var fs = FileSystem.of("local", Configuration.EMPTY)) {
      var iter =
          Consumer.forTopics(Set.copyOf(argument.topics))
              .bootstrapServers(argument.bootstrapServers())
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .config(ConsumerConfigs.GROUP_ID_CONFIG, argument.group)
              .iterator(List.of(IteratorLimit.idle(Duration.ofSeconds(3))));
      var stats = new HashMap<TopicPartition, Stat>();
      var writers = new HashMap<TopicPartition, RecordWriter>();
      while (iter.hasNext()) {
        var record = iter.next();
        var writer =
            writers.computeIfAbsent(
                record.topicPartition(),
                ignored -> {
                  var topicFolder = new File(argument.output.toFile(), record.topic());
                  var partitionFolder = new File(topicFolder, String.valueOf(record.partition()));
                  var file = new File(partitionFolder, String.valueOf(record.offset()));
                  return RecordWriter.builder(fs.write(file.getAbsolutePath())).build();
                });
        writer.append(record);
        // roll new writer in the future
        if (writer.size().greaterThan(argument.size)) {
          var stat = stats.computeIfAbsent(record.topicPartition(), Stat::new);
          stat.count.add(writer.count());
          stat.size.add(writer.size().bytes());
          writers.remove(record.topicPartition()).close();
        }
      }
      // close all writers
      writers.forEach(
          (tp, writer) -> {
            var stat = stats.computeIfAbsent(tp, Stat::new);
            stat.count.add(writer.count());
            stat.size.add(writer.size().bytes());
            writer.close();
          });
      return Collections.unmodifiableMap(stats);
    }
  }

  static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names to export data",
        validateWith = StringSetField.class,
        converter = StringSetField.class,
        required = true)
    Set<String> topics;

    @Parameter(
        names = {"--output"},
        description = "Path: the local folder to save data",
        converter = PathField.class,
        required = true)
    Path output;

    @Parameter(
        names = {"--group"},
        description =
            "String: the group id used by this exporter. You can run multiples exporter with same id in parallel")
    String group = Utils.randomString();

    @Parameter(
        names = {"--archive.size"},
        description = "DataSize: the max size of a archive file",
        converter = DataSizeField.class)
    DataSize size = DataSize.MB.of(100);
  }

  public static class Stat {
    public TopicPartition partition() {
      return partition;
    }

    public long count() {
      return count.sum();
    }

    public DataSize size() {
      return DataSize.Byte.of(size.sum());
    }

    private final TopicPartition partition;
    private final LongAdder count = new LongAdder();
    private final LongAdder size = new LongAdder();

    public Stat(TopicPartition partition) {
      this.partition = partition;
    }

    @Override
    public String toString() {
      return "Stat{" + "partition=" + partition + ", count=" + count + ", size=" + size + '}';
    }
  }
}
