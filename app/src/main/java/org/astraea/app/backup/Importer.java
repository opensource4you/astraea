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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.PathField;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;

public class Importer {

  public static void main(String[] args) {
    var arg = Argument.parse(new Argument(), args);
    System.out.println("prepare to import data from " + arg.input);
    var result = execute(arg);
    System.out.println(result);
  }

  public static Result execute(Argument argument) {
    if (!argument.input.toFile().isDirectory())
      throw new IllegalArgumentException("--input must be a existent folder");
    var files = new LinkedList<File>();
    files.add(argument.input.toFile());
    var recordCount = new HashMap<TopicPartition, Long>();
    try (var producer = Producer.of(argument.bootstrapServers())) {
      Consumer<File> process =
          file -> {
            var count = 0L;
            var iter = RecordReader.read(file);
            while (iter.hasNext()) {
              var record = iter.next();
              if (record.key() == null && record.value() == null) continue;
              producer.send(
                  Record.builder()
                      .topic(record.topic())
                      .partition(record.partition())
                      .key(record.key())
                      .value(record.value())
                      .timestamp(record.timestamp())
                      .headers(record.headers())
                      .build());
              recordCount.compute(
                  TopicPartition.of(record.topic(), record.partition()),
                  (k, v) -> v == null ? 1 : v + 1);
              count++;
            }
            System.out.println("succeed to import " + count + " records from " + file);
          };

      while (true) {
        var current = files.poll();
        if (current == null) break;
        if (current.isDirectory()) {
          var fs = current.listFiles();
          if (fs == null) continue;
          // add files first
          files.addAll(
              Arrays.stream(fs)
                  .filter(File::isFile)
                  .sorted(Comparator.comparing(f -> Long.parseLong(f.getName())))
                  .collect(Collectors.toList()));
          // add folders
          files.addAll(Arrays.stream(fs).filter(File::isDirectory).collect(Collectors.toList()));
        }
        if (current.isFile()) process.accept(current);
      }
    }
    return new Result(recordCount);
  }

  static class Argument extends org.astraea.common.argument.Argument {

    @Parameter(
        names = {"--input"},
        description = "Path: the local folder to offer data",
        converter = PathField.class,
        required = true)
    Path input;
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
