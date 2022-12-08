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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.consumer.Record;
import org.astraea.fs.FileSystem;

public class FtpSinkTask extends SinkTask {
  private FileSystem ftpClient;
  private Configuration cons;

  @Override
  protected void init(Configuration configuration) {
    this.ftpClient = FileSystem.of("ftp", configuration);
    this.cons = configuration;
  }

  @Override
  protected void put(List<Record<byte[], byte[]>> records) {
    var stats = new HashMap<TopicPartition, Stat>();
    var writers = new HashMap<TopicPartition, RecordWriter>();
    for (var record : records) {
      var writer =
          writers.computeIfAbsent(
              record.topicPartition(),
              ignored -> {
                var path = cons.requireString("path");
                var topicName = cons.requireString("topics");
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

      if (writer.size().greaterThan(DataSize.of(cons.requireString("size")))) {
        var stat = stats.computeIfAbsent(record.topicPartition(), Stat::new);
        stat.count.add(writer.count());
        stat.size.add(writer.size().bytes());
        writers.remove(record.topicPartition()).close();
      }
    }

    writers.forEach((tp, writer) -> writer.close());
  }

  @Override
  protected void close() {
    this.ftpClient.close();
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
