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
package org.astraea.app.homework;

import com.beust.jcommander.Parameter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;

public class SendYourData {

  private static final int NUMBER_OF_PARTITIONS = 4;

  public static void main(String[] args) throws IOException, InterruptedException {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) throws IOException, InterruptedException {
    try (var admin = Admin.of(param.bootstrapServers())) {
      param.topics.forEach(
          t ->
              admin
                  .creator()
                  .topic(t)
                  .numberOfReplicas((short) 1)
                  .numberOfPartitions(NUMBER_OF_PARTITIONS)
                  .run()
                  .toCompletableFuture()
                  .join());
      admin.waitCluster(
          Set.copyOf(param.topics),
          clusterInfo -> clusterInfo.topicNames().containsAll(param.topics),
          Duration.ofSeconds(10),
          1);
    }
    var keys =
        List.of(
            new Key(IntStream.range(0, 1000).mapToObj(Long::valueOf).toList()),
            new Key(IntStream.range(0, 2000).mapToObj(Long::valueOf).toList()),
            new Key(IntStream.range(0, 2500).mapToObj(Long::valueOf).toList()),
            new Key(IntStream.range(0, 3000).mapToObj(Long::valueOf).toList()));
    var max = Runtime.getRuntime().totalMemory();
    try (var sender = new YourSender(param.bootstrapServers())) {
      var start = System.currentTimeMillis();
      var fs =
          IntStream.range(0, 5)
              .mapToObj(
                  __ ->
                      CompletableFuture.runAsync(
                          () -> {
                            while (System.currentTimeMillis() - start
                                <= param.duration.toMillis()) {
                              keys.forEach(k -> sender.send(param.topics, k));
                            }
                          }))
              .toList();
      while (!fs.stream().allMatch(CompletableFuture::isDone)) {
        max = Math.max(max, Runtime.getRuntime().totalMemory());
        TimeUnit.MILLISECONDS.sleep(300);
      }
    }
    try (var admin = Admin.of(param.bootstrapServers())) {
      var offsets =
          param.topics.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      t -> t,
                      t ->
                          admin
                              .latestOffsets(
                                  IntStream.range(0, NUMBER_OF_PARTITIONS)
                                      .mapToObj(i -> TopicPartition.of(t, i))
                                      .collect(Collectors.toSet()))
                              .toCompletableFuture()
                              .join()
                              .values()
                              .stream()
                              .mapToLong(i -> i)
                              .sum()));
      System.out.println("memory=" + DataSize.Byte.of(max));
      System.out.println(
          "gc_count="
              + ManagementFactory.getGarbageCollectorMXBeans().stream()
                  .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                  .sum());
      System.out.println(
          "gc_time="
              + ManagementFactory.getGarbageCollectorMXBeans().stream()
                  .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                  .sum());
      offsets.forEach((t, o) -> System.out.println(t + "=" + o));
    }
  }

  public record Key(List<Long> vs) {}

  public static class YourSender implements Closeable {
    private final KafkaProducer<Key, byte[]> producer;

    @Override
    public void close() throws IOException {
      producer.close();
    }

    public YourSender(String bootstrapServers) {
      Serializer<Key> serializer =
          (topic, key) -> {
            var buffer = ByteBuffer.allocate(Long.BYTES * key.vs.size());
            key.vs.forEach(buffer::putLong);
            buffer.flip();
            var bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
          };
      producer =
          new KafkaProducer<>(
              Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
              serializer,
              new ByteArraySerializer());
    }

    public void send(List<String> topic, Key key) {
      topic.forEach(t -> producer.send(new ProducerRecord<>(t, key, null)));
    }
  }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you subscribed",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;

    @Parameter(
        names = {"--duration"},
        description = "duration: the time to test",
        validateWith = DurationField.class,
        converter = DurationField.class)
    Duration duration = Duration.ofSeconds(20);
  }
}
