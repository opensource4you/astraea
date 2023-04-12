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
import org.astraea.common.ByteUtils;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.ByteUtils;
import org.astraea.common.generated.BeanObjectOuterClass;
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
  Deserializer<NodeInfo> NODE_INFO = new NodeInfoDeserializer();
  Deserializer<Topic> TOPIC = new TopicDeserializer();
  Deserializer<Replica> REPLICA = new ReplicaDeserializer();
  Deserializer<ClusterInfo> CLUSTER_INFO = new ClusterInfoDeserializer();
  Deserializer<BeanObject> BEAN_OBJECT = (topic, headers, data) -> ByteUtils.readBeanObject(data);

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

  class ClusterInfoDeserializer implements Deserializer<NodeInfo> {
    @Override
    public ClusterInfo deserialize(String topic, List<Header> headers, byte[] data) {
      var outerClusterInfo =
          Utils.packException(() -> ClusterInfoOuterClass.ClusterInfo.parseFrom(data));
      return ClusterInfo.of(
          outerClusterInfo.getClusterId(),
          outerClusterInfo.getNodeInfoList().stream()
              .map(
                  nodeInfo -> NodeInfo.of(nodeInfo.getId(), nodeInfo.getHost(), nodeInfo.getPort()))
              .collect(Collectors.toList()),
          outerClusterInfo.getTopicList().stream()
              .map(
                  protoTopic ->
                      new Topic() {
                        @Override
                        public String name() {
                          return protoTopic.getName();
                        }

                        @Override
                        public Config config() {
                          return Config.of(protoTopic.getConfigMap());
                        }

                        @Override
                        public boolean internal() {
                          return protoTopic.getInternal();
                        }

                        @Override
                        public Set<TopicPartition> topicPartitions() {
                          return protoTopic.getTopicPartitionList().stream()
                              .map(tp -> TopicPartition.of(protoTopic.getName(), tp))
                              .collect(Collectors.toSet());
                        }
                      })
              .collect(Collectors.toMap(Topic::name, Function.identity())),
          outerClusterInfo.getReplicaList().stream()
              .map(
                  replica ->
                      Replica.builder()
                          .topic(replica.getTopic())
                          .partition(replica.getPartition())
                          .nodeInfo(
                              NodeInfo.of(
                                  replica.getNodeInfo().getId(),
                                  replica.getNodeInfo().getHost(),
                                  replica.getNodeInfo().getPort()))
                          .lag(replica.getLag())
                          .size(replica.getSize())
                          .isLeader(replica.getIsLeader())
                          .isSync(replica.getIsSync())
                          .isFuture(replica.getIsFuture())
                          .isOffline(replica.getIsOffline())
                          .isPreferredLeader(replica.getIsPreferredLeader())
                          .path(replica.getPath())
                          .build())
              .collect(Collectors.toList()));
    }
  }
}
