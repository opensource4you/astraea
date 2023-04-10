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
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.generated.BeanObjectOuterClass;
import org.astraea.common.generated.ClusterInfoOuterClass;
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

  class ClusterInfoSerializer implements Serializer<ClusterInfo> {

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, ClusterInfo data) {
      return ClusterInfoOuterClass.ClusterInfo.newBuilder()
          .setClusterId(data.clusterId())
          .addAllNodeInfo(
              data.nodes().stream()
                  .map(
                      nodeInfo ->
                          ClusterInfoOuterClass.ClusterInfo.NodeInfo.newBuilder()
                              .setId(nodeInfo.id())
                              .setHost(nodeInfo.host())
                              .setPort(nodeInfo.port())
                              .build())
                  .collect(Collectors.toList()))
          .addAllTopic(
              data.topics().values().stream()
                  .map(
                      topicClass ->
                          ClusterInfoOuterClass.ClusterInfo.Topic.newBuilder()
                              .setName(topicClass.name())
                              .putAllConfig(topicClass.config().raw())
                              .setInternal(topicClass.internal())
                              .addAllTopicPartition(
                                  topicClass.topicPartitions().stream()
                                      .map(TopicPartition::partition)
                                      .collect(Collectors.toList()))
                              .build())
                  .collect(Collectors.toList()))
          .addAllReplica(
              data.replicas().stream()
                  .map(
                      replica ->
                          ClusterInfoOuterClass.ClusterInfo.Replica.newBuilder()
                              .setTopic(replica.topic())
                              .setPartition(replica.partition())
                              .setNodeInfo(
                                  ClusterInfoOuterClass.ClusterInfo.NodeInfo.newBuilder()
                                      .setId(replica.nodeInfo().id())
                                      .setHost(replica.nodeInfo().host())
                                      .setPort(replica.nodeInfo().port())
                                      .build())
                              .setLag(replica.lag())
                              .setSize(replica.size())
                              .setIsLeader(replica.isLeader())
                              .setIsSync(replica.isSync())
                              .setIsFuture(replica.isFuture())
                              .setIsOffline(replica.isOffline())
                              .setIsPreferredLeader(replica.isPreferredLeader())
                              .setPath(replica.path())
                              .build())
                  .collect(Collectors.toList()))
          .build()
          .toByteArray();
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

    private static BeanObjectOuterClass.BeanObject.Primitive primitive(Object v) {
      if (v instanceof Integer)
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setInt((int) v).build();
      else if (v instanceof Long)
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setLong((long) v).build();
      else if (v instanceof Float)
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setFloat((float) v).build();
      else if (v instanceof Double)
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setDouble((double) v).build();
      else if (v instanceof Boolean)
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder()
            .setBoolean((boolean) v)
            .build();
      else if (v instanceof String)
        return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setStr(v.toString()).build();
      else
        throw new IllegalArgumentException(
            "Type "
                + v.getClass()
                + " is not supported. Please use Integer, Long, Float, Double, Boolean, String instead.");
    }
  }
}
