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
package org.astraea.common.backup;

import static org.astraea.common.consumer.SeekStrategy.DISTANCE_FROM_BEGINNING;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.IteratorLimit;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestReaderWriter extends RequireSingleBrokerCluster {

  private static void produceData(String topic, int size) {
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      IntStream.range(0, size)
          .forEach(
              i ->
                  producer.send(
                      Record.builder()
                          .topic(topic)
                          .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                          .build()));
      producer.flush();
    }
  }

  @Test
  void testReadWrite() throws IOException {
    var topic = Utils.randomString();
    produceData(topic, 100);
    var file = Files.createTempFile(topic, null).toFile();
    RecordWriter.write(
        file,
        (short) 0,
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, 0)))
            .bootstrapServers(bootstrapServers())
            .seek(DISTANCE_FROM_BEGINNING, 0)
            .iterator(List.of(IteratorLimit.count(100))));

    var iter = RecordReader.read(file);
    var count = 0;
    while (iter.hasNext()) {
      var record = iter.next();
      Assertions.assertEquals(topic, record.topic());
      Assertions.assertEquals(0, record.partition());
      Assertions.assertEquals(
          String.valueOf(count), new String(record.key(), StandardCharsets.UTF_8));
      count++;
    }
  }
}
