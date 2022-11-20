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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.PathField;
import org.astraea.common.argument.StringSetField;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.IteratorLimit;

public class Exporter {

  public static void main(String[] args) {
    var arg = Argument.parse(new Argument(), args);
    System.out.println("prepare to export data from " + arg.topics);
    var result = execute(arg);
    System.out.println(result);
  }

  public static Result execute(Argument argument) {
    if (!argument.output.toFile().isDirectory())
      throw new IllegalArgumentException("--output must be a existent folder");
    var root = argument.output.toFile();
    var recordCount = new HashMap<TopicPartition, Long>();
    for (var t : argument.topics) {
      var iter =
          Consumer.forTopics(Set.of(t))
              .bootstrapServers(argument.bootstrapServers())
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .config(ConsumerConfigs.GROUP_ID_CONFIG, argument.group)
              .iterator(List.of(IteratorLimit.idle(Duration.ofSeconds(3))));
      // skip empty iter to avoid creating empty file
      if (!iter.hasNext()) continue;
      // TODO: we should create the folder for each partition
      var file = new File(root, t);
      var count = 0;
      try (var writer = RecordWriter.builder(file).build()) {
        while (iter.hasNext()) {
          var record = iter.next();
          writer.append(record);
          count++;
          recordCount.compute(
              TopicPartition.of(record.topic(), record.partition()),
              (k, v) -> v == null ? 1 : v + 1);
        }
      }
      System.out.println("read " + count + " records from " + t);
    }
    return new Result(recordCount);
  }

  static class Argument extends org.astraea.common.argument.Argument {

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
  }

  public static class Result {
    private final Map<TopicPartition, Long> recordCount;

    private Result(Map<TopicPartition, Long> recordCount) {
      this.recordCount = recordCount;
    }

    public Map<TopicPartition, Long> recordCount() {
      return recordCount;
    }

    @Override
    public String toString() {
      return "Result{" + "recordCount=" + recordCount + '}';
    }
  }
}
