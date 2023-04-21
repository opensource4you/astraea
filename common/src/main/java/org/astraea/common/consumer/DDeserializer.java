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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.OldByteUtils;
import org.astraea.common.generated.BeanObjectOuterClass;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;

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
import java.util.stream.Stream;

@FunctionalInterface
public interface DDeserializer<T> {

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
      DDeserializer<T> deserializer) {
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

  private static <T> DDeserializer<T> of(
      org.apache.kafka.common.serialization.Deserializer<T> deserializer) {
    // the headers are not used by primitive type deserializer
    return (topic, headers, data) -> deserializer.deserialize(topic, data);
  }

  DDeserializer<String> BASE64 =
      (topic, headers, data) -> data == null ? null : Base64.getEncoder().encodeToString(data);
  DDeserializer<byte[]> BYTE_ARRAY = of(new ByteArrayDeserializer());
  DDeserializer<String> STRING = of(new StringDeserializer());
  DDeserializer<Integer> INTEGER = of(new IntegerDeserializer());
  DDeserializer<Long> LONG = of(new LongDeserializer());
  DDeserializer<Float> FLOAT = of(new FloatDeserializer());
  DDeserializer<Double> DOUBLE = of(new DoubleDeserializer());
  DDeserializer<BeanObject> BEAN_OBJECT = new BeanDeserializer();
  DDeserializer<NodeInfo> NODE_INFO = new NodeInfoDeserializer();
  DDeserializer<Topic> TOPIC = new TopicDeserializer();
  DDeserializer<Replica> REPLICA = new ReplicaDeserializer();
  DDeserializer<ClusterInfo> CLUSTER_INFO = new ClusterInfoDeserializer();

  /**
   * create Custom JsonDeserializer
   *
   * @param typeRef The typeRef of message being output by the Deserializer
   * @return Custom JsonDeserializer
   * @param <T> The type of message being output by the Deserializer
   */
  static <T> DDeserializer<T> of(TypeRef<T> typeRef) {
    return new JsonDeserializer<>(typeRef);
  }

  class JsonDeserializer<T> implements DDeserializer<T> {
    private final TypeRef<T> typeRef;
    private final JsonConverter jackson = JsonConverter.jackson();

    private JsonDeserializer(TypeRef<T> typeRef) {
      this.typeRef = typeRef;
    }

    @Override
    public T deserialize(String topic, List<Header> headers, byte[] data) {
      if (data == null) return null;
      else {
        return jackson.fromJson(DDeserializer.STRING.deserialize(topic, headers, data), typeRef);
      }
    }
  }

  /**
   * Deserialize byte arrays to string and then parse the string to `BeanObject`. It is inverse of
   * BeanObject.toString().getBytes(). TODO: Should be replaced by protoBuf
   */
  class BeanDeserializer implements DDeserializer<BeanObject> {
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

  class NodeInfoDeserializer implements DDeserializer<NodeInfo> {
    @Override
    public NodeInfo deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var id = buffer.getInt();
      var host = OldByteUtils.readString(buffer, buffer.getShort());
      var port = buffer.getInt();
      return NodeInfo.of(id, host, port);
    }
  }

  class TopicDeserializer implements DDeserializer<Topic> {

    @Override
    public Topic deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var name = OldByteUtils.readString(buffer, buffer.getShort());
      var config =
          IntStream.range(0, buffer.getInt())
              .boxed()
              .collect(
                  Collectors.toMap(
                      i -> OldByteUtils.readString(buffer, buffer.getShort()),
                      i -> OldByteUtils.readString(buffer, buffer.getShort())));
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

  class ReplicaDeserializer implements DDeserializer<Replica> {

    @Override
    public Replica deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var topicName = OldByteUtils.readString(buffer, buffer.getShort());
      var partition = buffer.getInt();
      var nodeInfoData = new byte[buffer.getInt()];
      buffer.get(nodeInfoData);
      var nodeInfo = DDeserializer.NODE_INFO.deserialize(topic, headers, nodeInfoData);
      var lag = buffer.getLong();
      var size = buffer.getLong();
      var isLeader = buffer.get() != 0;
      var isSync = buffer.get() != 0;
      var isFuture = buffer.get() != 0;
      var isOffline = buffer.get() != 0;
      var isPreferredLeader = buffer.get() != 0;
      var path = OldByteUtils.readString(buffer, buffer.getShort());
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

  class ClusterInfoDeserializer implements DDeserializer<ClusterInfo> {

    @Override
    public ClusterInfo deserialize(String topic, List<Header> headers, byte[] data) {
      var buffer = ByteBuffer.wrap(data);
      var clusterId = OldByteUtils.readString(buffer, buffer.getShort());
      var nodes =
          IntStream.range(0, buffer.getInt())
              .mapToObj(
                  i -> {
                    var nodeInfoData = new byte[buffer.getInt()];
                    buffer.get(nodeInfoData);
                    var node = DDeserializer.NODE_INFO.deserialize(topic, headers, nodeInfoData);

                    return new Broker() {
                      @Override
                      public boolean isController() {
                        return false;
                      }

                      @Override
                      public Config config() {
                        return null;
                      }

                      @Override
                      public List<DataFolder> dataFolders() {
                        return Stream.of(
                                "/tmp/log-folder-0", "/tmp/log-folder-1", "/tmp/log-folder-2")
                            .map(
                                path ->
                                    new DataFolder() {
                                      @Override
                                      public String path() {
                                        return path;
                                      }

                                      @Override
                                      public Map<TopicPartition, Long> partitionSizes() {
                                        return null;
                                      }

                                      @Override
                                      public Map<TopicPartition, Long> orphanPartitionSizes() {
                                        return null;
                                      }
                                    })
                            .collect(Collectors.toUnmodifiableList());
                      }

                      @Override
                      public Set<TopicPartition> topicPartitions() {
                        return null;
                      }

                      @Override
                      public Set<TopicPartition> topicPartitionLeaders() {
                        return null;
                      }

                      @Override
                      public String host() {
                        return node.host();
                      }

                      @Override
                      public int port() {
                        return node.port();
                      }

                      @Override
                      public int id() {
                        return node.id();
                      }
                    };
                  })
              .map(broker -> (NodeInfo) broker)
              .collect(Collectors.toUnmodifiableList());
      var topics =
          IntStream.range(0, buffer.getInt())
              .mapToObj(
                  i -> {
                    var topicData = new byte[buffer.getInt()];
                    buffer.get(topicData);
                    return DDeserializer.TOPIC.deserialize(topic, headers, topicData);
                  })
              .collect(Collectors.toMap(Topic::name, s -> s));
      var replicas =
          IntStream.range(0, buffer.getInt())
              .mapToObj(
                  i -> {
                    var replicaData = new byte[buffer.getInt()];
                    buffer.get(replicaData);
                    return DDeserializer.REPLICA.deserialize(topic, headers, replicaData);
                  })
              .collect(Collectors.toList());
      return ClusterInfo.of(clusterId, nodes, topics, replicas);
    }
  }

  class BeanObjectDeserializer implements DDeserializer<BeanObject> {
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

    private Object toObject(BeanObjectOuterClass.BeanObject.Primitive v) {
      var oneOfCase = v.getValueCase();
      switch (oneOfCase) {
        case INT:
          return v.getInt();
        case LONG:
          return v.getLong();
        case FLOAT:
          return v.getFloat();
        case DOUBLE:
          return v.getDouble();
        case BOOLEAN:
          return v.getBoolean();
        case STR:
          return v.getStr();
        case VALUE_NOT_SET:
        default:
          throw new IllegalArgumentException("The value is not set.");
      }
    }
  }
}
