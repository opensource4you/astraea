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
package org.astraea.common.consumer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.astraea.common.BeanObjectOuterClass;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.ByteUtils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;

@FunctionalInterface
public interface Deserializer<T> {

  /**
   * Deserialize a record value from a byte array into a value or object.
   *
   * @param topic topic associated with the data
   * @param headers headers associated with the record; may be empty.
   * @param data serialized bytes; may be null; implementations are recommended to handle null by
   *     returning a value or null rather than throwing an exception.
   * @return deserialized typed data; may be null
   */
  T deserialize(String topic, List<Header> headers, byte[] data);

  static <T> org.apache.kafka.common.serialization.Deserializer<T> of(
      Deserializer<T> deserializer) {
    return new org.apache.kafka.common.serialization.Deserializer<>() {

      @Override
      public T deserialize(String topic, byte[] data) {
        return deserializer.deserialize(topic, List.of(), data);
      }

      @Override
      public T deserialize(String topic, Headers headers, byte[] data) {
        return deserializer.deserialize(topic, Header.of(headers), data);
      }
    };
  }

  private static <T> Deserializer<T> of(
      org.apache.kafka.common.serialization.Deserializer<T> deserializer) {
    // the headers are not used by primitive type deserializer
    return (topic, headers, data) -> deserializer.deserialize(topic, data);
  }

  Deserializer<String> BASE64 =
      (topic, headers, data) -> data == null ? null : Base64.getEncoder().encodeToString(data);
  Deserializer<byte[]> BYTE_ARRAY = of(new ByteArrayDeserializer());
  Deserializer<String> STRING = of(new StringDeserializer());
  Deserializer<Integer> INTEGER = of(new IntegerDeserializer());
  Deserializer<Long> LONG = of(new LongDeserializer());
  Deserializer<Float> FLOAT = of(new FloatDeserializer());
  Deserializer<Double> DOUBLE = of(new DoubleDeserializer());
  Deserializer<BeanObject> BEAN_OBJECT = new BeanDeserializer();
  Deserializer<NodeInfo> NODE_INFO = new NodeInfoDeserializer();
  Deserializer<Topic> TOPIC = new TopicDeserializer();
  Deserializer<Replica> REPLICA = new ReplicaDeserializer();
  Deserializer<ClusterInfo> CLUSTER_INFO = new ClusterInfoDeserializer();

  /**
   * create Custom JsonDeserializer
   *
   * @param typeRef The typeRef of message being output by the Deserializer
   * @return Custom JsonDeserializer
   * @param <T> The type of message being output by the Deserializer
   */
  static <T> Deserializer<T> of(TypeRef<T> typeRef) {
    return new JsonDeserializer<>(typeRef);
  }

  class JsonDeserializer<T> implements Deserializer<T> {
    private final TypeRef<T> typeRef;
    private final JsonConverter jackson = JsonConverter.jackson();

    private JsonDeserializer(TypeRef<T> typeRef) {
      this.typeRef = typeRef;
    }

    @Override
    public T deserialize(String topic, List<Header> headers, byte[] data) {
      if (data == null) return null;
      else {
        return jackson.fromJson(Deserializer.STRING.deserialize(topic, headers, data), typeRef);
      }
    }
  }

  /**
   * Deserialize byte arrays to string and then parse the string to `BeanObject`. It is inverse of
   * BeanObject.toString().getBytes(). TODO: Should be replaced by protoBuf
   */
  class BeanDeserializer implements Deserializer<BeanObject> {
    @Override
    public BeanObject deserialize(String topic, List<Header> headers, byte[] data) {
      var beanString = new String(data);
      Pattern p =
          Pattern.compile("\\[(?<domain>[^:]*):(?<properties>[^]]*)]\n\\{(?<attributes>[^}]*)}");
      Matcher m = p.matcher(beanString);
      if (!m.matches()) return null;
      var domain = m.group("domain");
      var propertiesPairs = m.group("properties").split("[, ]");
      var attributesPairs = m.group("attributes").split("[, ]");
      var properties =
          Arrays.stream(propertiesPairs)
              .map(kv -> kv.split("="))
              .filter(kv -> kv.length >= 2)
              .collect(Collectors.toUnmodifiableMap(kv -> kv[0], kv -> kv[1]));
      var attributes =
          Arrays.stream(attributesPairs)
              .map(kv -> kv.split("="))
              .filter(kv -> kv.length >= 2)
              .collect(Collectors.toUnmodifiableMap(kv -> kv[0], kv -> (Object) kv[1]));
      return new BeanObject(domain, properties, attributes);
    }
  }

  class NodeInfoDeserializer implements Deserializer<NodeInfo> {
    @Override
    public NodeInfo deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var id = buffer.getInt();
      var host = ByteUtils.readString(buffer, buffer.getShort());
      var port = buffer.getInt();
      return NodeInfo.of(id, host, port);
    }
  }

  class TopicDeserializer implements Deserializer<Topic> {

    @Override
    public Topic deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var name = ByteUtils.readString(buffer, buffer.getShort());
      var config =
          IntStream.range(0, buffer.getInt())
              .boxed()
              .collect(
                  Collectors.toMap(
                      i -> ByteUtils.readString(buffer, buffer.getShort()),
                      i -> ByteUtils.readString(buffer, buffer.getShort())));
      var internal = buffer.get() != 0;
      var topicPartitions =
          IntStream.range(0, buffer.getInt())
              .mapToObj(i -> TopicPartition.of(name, buffer.getInt()))
              .collect(Collectors.toSet());
      return new Topic() {
        @Override
        public String name() {
          return name;
        }

        @Override
        public Config config() {
          return Config.of(config);
        }

        @Override
        public boolean internal() {
          return internal;
        }

        @Override
        public Set<TopicPartition> topicPartitions() {
          return topicPartitions;
        }
      };
    }
  }

  class ReplicaDeserializer implements Deserializer<Replica> {

    @Override
    public Replica deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var topicName = ByteUtils.readString(buffer, buffer.getShort());
      var partition = buffer.getInt();
      var nodeInfoData = new byte[buffer.getInt()];
      buffer.get(nodeInfoData);
      var nodeInfo = Deserializer.NODE_INFO.deserialize(topic, headers, nodeInfoData);
      var lag = buffer.getLong();
      var size = buffer.getLong();
      var isLeader = buffer.get() != 0;
      var isSync = buffer.get() != 0;
      var isFuture = buffer.get() != 0;
      var isOffline = buffer.get() != 0;
      var isPreferredLeader = buffer.get() != 0;
      var path = ByteUtils.readString(buffer, buffer.getShort());
      return Replica.builder()
          .topic(topicName)
          .partition(partition)
          .nodeInfo(nodeInfo)
          .lag(lag)
          .size(size)
          .isLeader(isLeader)
          .isSync(isSync)
          .isFuture(isFuture)
          .isOffline(isOffline)
          .isPreferredLeader(isPreferredLeader)
          .path(path)
          .build();
    }
  }

  class ClusterInfoDeserializer implements Deserializer<ClusterInfo> {

    @Override
    public ClusterInfo deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var clusterId = ByteUtils.readString(buffer, buffer.getShort());
      var nodes =
          IntStream.range(0, buffer.getInt())
              .mapToObj(
                  i -> {
                    var nodeInfoData = new byte[buffer.getInt()];
                    buffer.get(nodeInfoData);
                    return Deserializer.NODE_INFO.deserialize(topic, headers, nodeInfoData);
                  })
              .collect(Collectors.toUnmodifiableList());
      var topics =
          IntStream.range(0, buffer.getInt())
              .mapToObj(
                  i -> {
                    var topicData = new byte[buffer.getInt()];
                    buffer.get(topicData);
                    return Deserializer.TOPIC.deserialize(topic, headers, topicData);
                  })
              .collect(Collectors.toMap(Topic::name, s -> s));
      var replicas =
          IntStream.range(0, buffer.getInt())
              .mapToObj(
                  i -> {
                    var replicaData = new byte[buffer.getInt()];
                    buffer.get(replicaData);
                    return Deserializer.REPLICA.deserialize(topic, headers, replicaData);
                  })
              .collect(Collectors.toList());
      return ClusterInfo.of(clusterId, nodes, topics, replicas);
    }
  }

  class BeanObjectDeserializer implements Deserializer<BeanObject> {
    @Override
    public BeanObject deserialize(String topic, List<Header> headers, byte[] data) {
      // Pack InvalidProtocolBufferException thrown by protoBuf
      var outerBean = Utils.packException(() -> BeanObjectOuterClass.BeanObject.parseFrom(data));
      return new BeanObject(
          outerBean.getDomain(),
          outerBean.getPropertiesMap(),
          outerBean.getAttributesMap().entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      Map.Entry::getKey, e -> Objects.requireNonNull(toObject(e.getValue())))));
    }

    private Object toObject(BeanObjectOuterClass.BeanObject.Primitive primitive) {
      var oneOfCase = primitive.getValueCase();
      switch (oneOfCase) {
        case INT:
          return primitive.getInt();
        case LONG:
          return primitive.getLong();
        case FLOAT:
          return primitive.getFloat();
        case DOUBLE:
          return primitive.getDouble();
        case BOOLEAN:
          return primitive.getBoolean();
        case STR:
          return primitive.getStr();
        case VALUE_NOT_SET:
        default:
          return null;
      }
    }
  }
}
