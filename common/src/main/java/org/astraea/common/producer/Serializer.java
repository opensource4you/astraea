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
package org.astraea.common.producer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.common.BeanObjectOuterClass;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.Topic;
import org.astraea.common.backup.ByteUtils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;

@FunctionalInterface
public interface Serializer<T> {

  /**
   * Convert {@code data} into a byte array.
   *
   * @param topic topic associated with data
   * @param headers headers associated with the record
   * @param data typed data
   * @return serialized bytes
   */
  byte[] serialize(String topic, Collection<Header> headers, T data);

  static <T> org.apache.kafka.common.serialization.Serializer<T> of(Serializer<T> serializer) {
    return new org.apache.kafka.common.serialization.Serializer<>() {

      @Override
      public byte[] serialize(String topic, T data) {
        return serializer.serialize(topic, List.of(), data);
      }

      @Override
      public byte[] serialize(String topic, Headers headers, T data) {
        return serializer.serialize(topic, Header.of(headers), data);
      }
    };
  }

  private static <T> Serializer<T> of(
      org.apache.kafka.common.serialization.Serializer<T> serializer) {
    // the headers are not used by primitive type serializer
    return (topic, headers, data) -> serializer.serialize(topic, data);
  }

  Serializer<byte[]> BYTE_ARRAY = of(new ByteArraySerializer());
  Serializer<String> STRING = of(new StringSerializer());
  Serializer<Integer> INTEGER = of(new IntegerSerializer());
  Serializer<Long> LONG = of(new LongSerializer());
  Serializer<Float> FLOAT = of(new FloatSerializer());
  Serializer<Double> DOUBLE = of(new DoubleSerializer());
  Serializer<NodeInfo> NODE_INFO = new NodeInfoSerializer();
  Serializer<Topic> TOPIC = new TopicSerializer();
  Serializer<Replica> REPLICA = new ReplicaSerializer();
  Serializer<ClusterInfo> CLUSTER_INFO = new ClusterInfoSerializer();

  /**
   * create Custom JsonSerializer
   *
   * @return Custom JsonSerializer
   * @param <T> The type of message being output by the serializer
   */
  static <T> Serializer<T> of(TypeRef<T> typeRef) {
    return new JsonSerializer<>(typeRef);
  }

  class JsonSerializer<T> implements Serializer<T> {
    private final String encoding = StandardCharsets.UTF_8.name();
    private final JsonConverter jackson = JsonConverter.jackson();

    private JsonSerializer(TypeRef<T> typeRef) {}

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, T data) {
      if (data == null) {
        return null;
      }

      return Utils.packException(() -> jackson.toJson(data).getBytes(encoding));
    }
  }

  class NodeInfoSerializer implements Serializer<NodeInfo> {

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, NodeInfo data) {
      var nodeInfoSize =
          4 // [id]
              + 2 // [host length]
              + ByteUtils.toBytes(data.host()).length // [host]
              + 4; // [port]
      var buffer = ByteBuffer.allocate(nodeInfoSize);
      buffer.putInt(data.id());
      ByteUtils.putLengthString(buffer, data.host());
      buffer.putInt(data.port());
      return buffer.array();
    }
  }

  class TopicSerializer implements Serializer<Topic> {

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, Topic data) {
      var configSize =
          data.config().raw().entrySet().stream()
              .mapToInt(
                  entry ->
                      2
                          + ByteUtils.toBytes(entry.getKey()).length
                          + 2
                          + ByteUtils.toBytes(entry.getValue()).length)
              .sum();
      var topicSize =
          2 // [topic length]
              + ByteUtils.toBytes(data.name()).length // [topic]
              + 4 // [nums of config]
              + configSize // [config]
              + 1 // [internal]
              + 4 // [nums of topicPartitions]
              + 4 * 4; // [nums * partitions]
      var buffer = ByteBuffer.allocate(topicSize);
      ByteUtils.putLengthString(buffer, data.name());
      buffer.putInt(data.config().raw().size());
      data.config()
          .raw()
          .forEach(
              (key, value) -> {
                ByteUtils.putLengthString(buffer, key);
                ByteUtils.putLengthString(buffer, value);
              });
      buffer.put(ByteUtils.toBytes(data.internal()));
      buffer.putInt(data.topicPartitions().size());
      data.topicPartitions().forEach(tp -> buffer.putInt(tp.partition()));
      return buffer.array();
    }
  }

  class ReplicaSerializer implements Serializer<Replica> {

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, Replica data) {
      var nodeInfo = Serializer.NODE_INFO.serialize(topic, headers, data.nodeInfo());
      var replicaSize =
          2 // [topic length]
              + ByteUtils.toBytes(topic).length // [topic]
              + 4 // [partition]
              + 4 // [nodeInfo length]
              + nodeInfo.length // [nodeInfo]
              + 8 // [lag]
              + 8 // [size]
              + 1 // [isLeader]
              + 1 // [isSync]
              + 1 // [isFuture]
              + 1 // [isOffline]
              + 1 // [isPreferredLeader]
              + 2 // [path length]
              + ByteUtils.toBytes(data.path()).length; // [path]
      var buffer = ByteBuffer.allocate(replicaSize);
      ByteUtils.putLengthString(buffer, topic);
      buffer.putInt(data.partition());
      buffer.putInt(nodeInfo.length);
      buffer.put(nodeInfo);
      buffer.putLong(data.lag());
      buffer.putLong(data.size());
      buffer.put(ByteUtils.toBytes(data.isLeader()));
      buffer.put(ByteUtils.toBytes(data.isSync()));
      buffer.put(ByteUtils.toBytes(data.isFuture()));
      buffer.put(ByteUtils.toBytes(data.isOffline()));
      buffer.put(ByteUtils.toBytes(data.isPreferredLeader()));
      ByteUtils.putLengthString(buffer, data.path());
      return buffer.array();
    }
  }

  class ClusterInfoSerializer implements Serializer<ClusterInfo> {

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, ClusterInfo data) {
      var nodes =
          data.nodes().stream()
              .map(nodeInfo -> Serializer.NODE_INFO.serialize(topic, headers, nodeInfo))
              .collect(Collectors.toList());
      var topics =
          data.topics().values().stream()
              .map(t -> Serializer.TOPIC.serialize(topic, headers, t))
              .collect(Collectors.toList());
      var replicas =
          data.replicas().stream()
              .map(replica -> Serializer.REPLICA.serialize(replica.topic(), headers, replica))
              .collect(Collectors.toList());
      var clusterInfoSize =
          2 // [clusterId length]
              + ByteUtils.toBytes(data.clusterId()).length // [clusterId]
              + 4 // [nums of nodeInfo]
              + nodes.stream().mapToInt(bytes -> 4 + bytes.length).sum() // [nodeInfo]
              + 4 // [nums of topic]
              + topics.stream().mapToInt(bytes -> 4 + bytes.length).sum() // [topics]
              + 4 // [nums of replica]
              + replicas.stream().mapToInt(bytes -> 4 + bytes.length).sum(); // [replicas]
      var buffer = ByteBuffer.allocate(clusterInfoSize);
      ByteUtils.putLengthString(buffer, data.clusterId());
      buffer.putInt(nodes.size());
      nodes.forEach(
          bytes -> {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
          });
      buffer.putInt(topics.size());
      topics.forEach(
          bytes -> {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
          });
      buffer.putInt(replicas.size());
      replicas.forEach(
          bytes -> {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
          });
      return buffer.array();
    }
  }

  class BeanObjectSerializer implements Serializer<BeanObject> {
    @Override
    public byte[] serialize(String topic, Collection<Header> headers, BeanObject data) {
      var beanBuilder = BeanObjectOuterClass.BeanObject.newBuilder();
      beanBuilder.setDomain(data.domainName());
      beanBuilder.putAllProperties(data.properties());
      data.attributes().forEach((key, val) -> beanBuilder.putAttributes(key, primitive(val)));

      return beanBuilder.build().toByteArray();
    }

    private BeanObjectOuterClass.BeanObject.Primitive primitive(Object v) {
      if (v instanceof Integer) {
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setInt((int) v).build();
      } else if (v instanceof Long) {
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setLong((long) v).build();
      } else if (v instanceof Float) {
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setFloat((float) v).build();
      } else if (v instanceof Double) {
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setDouble((double) v).build();
      } else if (v instanceof Boolean) {
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder()
            .setBoolean((boolean) v)
            .build();
      } else {
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setStr(v.toString()).build();
      }
    }
  }
}
