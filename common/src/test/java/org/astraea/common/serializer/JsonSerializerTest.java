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
package org.astraea.common.serializer;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.json.TypeRef;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.astraea.common.serialization.JsonDeserializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JsonSerializerTest extends RequireBrokerCluster {
  @Test
  void testJson() {
    String topic = createTopic();
    Utils.sleep(Duration.ofSeconds(1));
    try (var producer =
            Producer.builder()
                .bootstrapServers(bootstrapServers())
                .keySerializer(Serializer.JSON)
                .build();
        var consumer =
            Consumer.forTopics(Set.of(topic))
                .bootstrapServers(bootstrapServers())
                .keyDeserializer(Deserializer.STRING)
                .config(
                    ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                    ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                .build()) {
      producer
          .send(
              Record.builder()
                  .topic(topic)
                  .key((Object) Map.of("name", "ben", "age", "22"))
                  .build())
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var records = consumer.poll(2, Duration.ofSeconds(5));
      if (!records.isEmpty())
        Assertions.assertEquals(
            records.stream().findFirst().get().key(), "{\"age\":\"22\",\"name\":\"ben\"}");
    }
    Map<String, Object> config =
        Map.of(
            ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG,
            requireNonNull(bootstrapServers()),
            ConsumerConfigs.GROUP_ID_CONFIG,
            "1",
            ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST,
            ConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.astraea.common.serialization.JsonDeserializer",
            ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.astraea.common.serialization.JsonDeserializer",
            "deserializer.type",
            "map");
    try (var consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(Set.of(topic));
      var records = consumer.poll(Duration.ofSeconds(5)).records(topic).iterator();
      if (records.hasNext()) {
        Map<String, String> key = (Map<String, String>) records.next().key();
        Assertions.assertEquals("22", key.get("age"));
        Assertions.assertEquals("ben", key.get("name"));
      }
    }
  }

  @Test
  void testPrimitiveJson() {
    var testFieldClass = new TestPrimitiveClass();
    testFieldClass.doubleValue = 456d;
    testFieldClass.intValue = 12;
    testFieldClass.stringValue = "hello";

    String topic = createTopic();
    Utils.sleep(Duration.ofSeconds(1));
    try (var producer =
            Producer.builder()
                .bootstrapServers(bootstrapServers())
                .keySerializer(Serializer.JSON)
                .build();
        var consumer =
            Consumer.forTopics(Set.of(topic))
                .bootstrapServers(bootstrapServers())
                .keyDeserializer(Deserializer.STRING)
                .config(
                    ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                    ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                .build()) {

      producer
          .send(Record.builder().topic(topic).key((Object) testFieldClass).build())
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var records = consumer.poll(2, Duration.ofSeconds(5));
      if (!records.isEmpty())
        Assertions.assertEquals(
            "{\"doubleValue\":456.0,\"intValue\":12,\"stringValue\":\"hello\"}",
            records.stream().findFirst().get().key());
    }
    Map<String, Object> config =
        Map.of(
            ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG,
            requireNonNull(bootstrapServers()),
            ConsumerConfigs.GROUP_ID_CONFIG,
            "1",
            ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST,
            ConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.astraea.common.serialization.JsonDeserializer",
            ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.astraea.common.serialization.JsonDeserializer",
            "deserializer.type",
            TypeRef.of(TestPrimitiveClass.class));
    try (var consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(Set.of(topic));
      var records = consumer.poll(Duration.ofSeconds(5)).records(topic).iterator();
      if (records.hasNext()) {
        TestPrimitiveClass key = (TestPrimitiveClass) records.next().key();
        Assertions.assertEquals(456d, key.doubleValue);
        Assertions.assertEquals(12, key.intValue);
        Assertions.assertEquals("hello", key.stringValue);
      }
    }
  }

  @Test
  void testErrorDeserializationConfig() {
    Map<String, Object> errorType =
        Map.of(
            ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.astraea.common.serialization.JsonDeserializer",
            "deserializer.type",
            123);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new JsonDeserializer().configure(errorType, true));
    Map<String, Object> nullType =
        Map.of(
            ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.astraea.common.serialization.JsonDeserializer");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new JsonDeserializer().configure(nullType, true));
  }

  private static String createTopic() {
    var topic = "topic" + Utils.randomString(5);
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
    }
    return topic;
  }

  private static class TestPrimitiveClass {
    private String stringValue;
    private int intValue;
    private Double doubleValue;
  }
}
