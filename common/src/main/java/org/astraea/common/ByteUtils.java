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
package org.astraea.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.generated.BeanObjectOuterClass;
import org.astraea.common.generated.ClusterInfoOuterClass;
import org.astraea.common.generated.PrimitiveOuterClass;
import org.astraea.common.metrics.BeanObject;

public final class ByteUtils {

  // ----------------------------------[Java Primitive]----------------------------------//

  public static byte[] toBytes(short value) {
    return new byte[] {(byte) (value >>> 8), (byte) value};
  }

  public static short toShort(byte[] value) {
    if (value.length != Short.BYTES) {
      throw new IllegalArgumentException(
          "expected size: " + Short.BYTES + " but actual: " + value.length);
    }
    short r = 0;
    for (byte b : value) {
      r <<= 8;
      r |= b & 0xFF;
    }
    return r;
  }

  public static byte[] toBytes(int value) {
    return new byte[] {
      (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
    };
  }

  public static int toInteger(byte[] value) {
    if (value.length != Integer.BYTES) {
      throw new IllegalArgumentException(
          "expected size: " + Integer.BYTES + " but actual: " + value.length);
    }
    int r = 0;
    for (byte b : value) {
      r <<= 8;
      r |= b & 0xFF;
    }
    return r;
  }

  public static byte[] toBytes(long value) {
    return new byte[] {
      (byte) (value >>> 56),
      (byte) (value >>> 48),
      (byte) (value >>> 40),
      (byte) (value >>> 32),
      (byte) (value >>> 24),
      (byte) (value >>> 16),
      (byte) (value >>> 8),
      (byte) value
    };
  }

  public static long toLong(byte[] value) {
    if (value.length != Long.BYTES) {
      throw new IllegalArgumentException(
          "expected size: " + Long.BYTES + " but actual: " + value.length);
    }
    long r = 0;
    for (byte b : value) {
      r <<= 8;
      r |= b & 0xFF;
    }
    return r;
  }

  public static byte[] toBytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  public static String toString(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  public static byte[] toBytes(float value) {
    int intBits = Float.floatToIntBits(value);
    return new byte[] {
      (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) intBits
    };
  }

  public static float toFloat(byte[] value) {
    if (value.length != Float.BYTES) {
      throw new IllegalArgumentException(
          "expected size: " + Float.BYTES + " but actual: " + value.length);
    }
    int r = 0;
    for (byte b : value) {
      r <<= 8;
      r |= b & 0xFF;
    }
    return Float.intBitsToFloat(r);
  }

  public static byte[] toBytes(double value) {
    long longBits = Double.doubleToLongBits(value);
    return new byte[] {
      (byte) (longBits >> 56),
      (byte) (longBits >> 48),
      (byte) (longBits >> 40),
      (byte) (longBits >> 32),
      (byte) (longBits >> 24),
      (byte) (longBits >> 16),
      (byte) (longBits >> 8),
      (byte) longBits
    };
  }

  public static double toDouble(byte[] value) {
    if (value.length != Double.BYTES) {
      throw new IllegalArgumentException(
          "expected size: " + Double.BYTES + " but actual: " + value.length);
    }
    long r = 0;
    for (byte b : value) {
      r <<= 8;
      r |= b & 0xFF;
    }
    return Double.longBitsToDouble(r);
  }

  public static byte[] toBytes(boolean value) {
    if (value) return new byte[] {1};
    return new byte[] {0};
  }

  /** Serialize BeanObject by protocol buffer. The unsupported value will be ignored. */
  public static byte[] toBytes(BeanObject value) {
    var beanBuilder = BeanObjectOuterClass.BeanObject.newBuilder();
    beanBuilder.setDomain(value.domainName());
    beanBuilder.putAllProperties(value.properties());
    value
        .attributes()
        .forEach(
            (key, val) -> {
              try {
                beanBuilder.putAttributes(key, primitive(val));
              } catch (SerializationException ignore) {
                // Bean attribute may contain non-primitive value. e.g. TimeUnit, Byte.
              }
            });
    beanBuilder.setCreatedTimestamp(
        Timestamp.newBuilder()
            .setSeconds(value.createdTimestamp() / 1000)
            .setNanos((int) (value.createdTimestamp() % 1000) * 1000000));
    return beanBuilder.build().toByteArray();
  }

  // TODO: Due to the change of NodeInfo to Broker. This and the test should be updated.
  /** Serialize ClusterInfo by protocol buffer. */
  public static byte[] toBytes(ClusterInfo value) {
    return ClusterInfoOuterClass.ClusterInfo.newBuilder()
        .setClusterId(value.clusterId())
        .addAllNodeInfo(
            value.brokers().stream()
                .map(
                    nodeInfo ->
                        ClusterInfoOuterClass.ClusterInfo.NodeInfo.newBuilder()
                            .setId(nodeInfo.id())
                            .setHost(nodeInfo.host())
                            .setPort(nodeInfo.port())
                            .build())
                .collect(Collectors.toList()))
        .addAllTopic(
            value.topics().values().stream()
                .map(
                    topicClass ->
                        ClusterInfoOuterClass.ClusterInfo.Topic.newBuilder()
                            .setName(topicClass.name())
                            .putAllConfig(topicClass.config().raw())
                            .setInternal(topicClass.internal())
                            .addAllPartition(
                                topicClass.topicPartitions().stream()
                                    .map(TopicPartition::partition)
                                    .collect(Collectors.toList()))
                            .build())
                .collect(Collectors.toList()))
        .addAllReplica(
            value.replicas().stream()
                .map(
                    replica ->
                        ClusterInfoOuterClass.ClusterInfo.Replica.newBuilder()
                            .setTopic(replica.topic())
                            .setPartition(replica.partition())
                            .setNodeInfo(
                                ClusterInfoOuterClass.ClusterInfo.NodeInfo.newBuilder()
                                    .setId(replica.broker().id())
                                    .setHost(replica.broker().host())
                                    .setPort(replica.broker().port())
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

  public static int readInt(ReadableByteChannel channel) {
    return Utils.packException(
        () -> {
          var buf = ByteBuffer.allocate(Integer.BYTES);
          var size = channel.read(buf);
          if (size != Integer.BYTES)
            throw new IllegalStateException(
                "The remaining size is " + size + ", but expected is " + Integer.BYTES);
          return buf.flip().getInt();
        });
  }

  public static int readInt(InputStream fs) {
    return Utils.packException(
        () -> {
          var byteArray = new byte[Integer.BYTES];
          var size = fs.read(byteArray);
          if (size != Integer.BYTES)
            throw new IllegalStateException(
                "The remaining size is " + size + ", but expected is " + Integer.BYTES);
          return ByteBuffer.wrap(byteArray).getInt();
        });
  }

  public static short readShort(ReadableByteChannel channel) {
    return Utils.packException(
        () -> {
          var buf = ByteBuffer.allocate(Short.BYTES);
          var size = channel.read(buf);
          if (size != Short.BYTES)
            throw new IllegalStateException(
                "The remaining size is " + size + ", but expected is " + Short.BYTES);
          return buf.flip().getShort();
        });
  }

  public static short readShort(InputStream fs) {
    return Utils.packException(
        () -> {
          var byteArray = new byte[Short.BYTES];
          var size = fs.read(byteArray);
          if (size != Short.BYTES)
            throw new IllegalStateException(
                "The remaining size is " + size + ", but expected is " + Short.BYTES);
          return ByteBuffer.wrap(byteArray).getShort();
        });
  }

  public static String readString(ByteBuffer buffer, int size) {
    if (size < 0) return null;
    var dst = new byte[size];
    buffer.get(dst);
    return new String(dst, StandardCharsets.UTF_8);
  }

  /**
   * @return null if the size is smaller than zero
   */
  public static byte[] readBytes(ByteBuffer buffer, int size) {
    if (size < 0) return null;
    var dst = new byte[size];
    buffer.get(dst);
    return dst;
  }

  // ---------------------------------ProtoBuf Object------------------------------------------- //

  /** Deserialize to BeanObject with protocol buffer */
  public static BeanObject readBeanObject(byte[] bytes) throws SerializationException {
    try {
      var outerBean = BeanObjectOuterClass.BeanObject.parseFrom(bytes);
      return new BeanObject(
          outerBean.getDomain(),
          outerBean.getPropertiesMap(),
          outerBean.getAttributesMap().entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      Map.Entry::getKey, e -> Objects.requireNonNull(toObject(e.getValue())))),
          outerBean.getCreatedTimestamp().getSeconds() * 1000
              + outerBean.getCreatedTimestamp().getNanos() / 1000000);
    } catch (InvalidProtocolBufferException ex) {
      // Pack exception thrown by protoBuf to Serialization exception.
      throw new SerializationException(ex);
    }
  }

  // TODO: Due to the change of NodeInfo to Broker. This and the test should be updated.
  /** Deserialize to ClusterInfo with protocol buffer */
  public static ClusterInfo readClusterInfo(byte[] bytes) {
    try {
      var outerClusterInfo = ClusterInfoOuterClass.ClusterInfo.parseFrom(bytes);
      return ClusterInfo.of(
          outerClusterInfo.getClusterId(),
          outerClusterInfo.getNodeInfoList().stream()
              .map(nodeInfo -> Broker.of(nodeInfo.getId(), nodeInfo.getHost(), nodeInfo.getPort()))
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
                          return new Config(protoTopic.getConfigMap());
                        }

                        @Override
                        public boolean internal() {
                          return protoTopic.getInternal();
                        }

                        @Override
                        public Set<TopicPartition> topicPartitions() {
                          return protoTopic.getPartitionList().stream()
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
                          .broker(
                              Broker.of(
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
    } catch (InvalidProtocolBufferException ex) {
      throw new SerializationException(ex);
    }
  }

  // --------------------------------ProtoBuf Primitive----------------------------------------- //

  /**
   * Convert java primitive type to "one of" protocol buffer primitive type. There are no "short"
   * and "char" in Protocol Buffers. Use "int" and "String" instead.
   */
  private static PrimitiveOuterClass.Primitive primitive(Object v) throws SerializationException {
    if (v instanceof Integer)
      return PrimitiveOuterClass.Primitive.newBuilder().setInt((int) v).build();
    else if (v instanceof Long)
      return PrimitiveOuterClass.Primitive.newBuilder().setLong((long) v).build();
    else if (v instanceof Float)
      return PrimitiveOuterClass.Primitive.newBuilder().setFloat((float) v).build();
    else if (v instanceof Double)
      return PrimitiveOuterClass.Primitive.newBuilder().setDouble((double) v).build();
    else if (v instanceof Boolean)
      return PrimitiveOuterClass.Primitive.newBuilder().setBoolean((boolean) v).build();
    else if (v instanceof String)
      return PrimitiveOuterClass.Primitive.newBuilder().setStr(v.toString()).build();
    else
      throw new SerializationException(
          "Type "
              + v.getClass()
              + " is not supported. Please use Integer, Long, Float, Double, Boolean, String instead.");
  }

  /** Retrieve field from "one of" field. */
  private static Object toObject(PrimitiveOuterClass.Primitive v) {
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

  // ------------------------------------ByteBuffer--------------------------------------------- //

  public static ByteBuffer of(short value) {
    var buf = ByteBuffer.allocate(Short.BYTES);
    buf.putShort(value);
    return buf.flip();
  }

  public static ByteBuffer of(int value) {
    var buf = ByteBuffer.allocate(Integer.BYTES);
    buf.putInt(value);
    return buf.flip();
  }

  public static void putLengthBytes(ByteBuffer buffer, byte[] value) {
    if (value == null) buffer.putInt(-1);
    else {
      buffer.putInt(value.length);
      buffer.put(ByteBuffer.wrap(value));
    }
  }

  public static void putLengthString(ByteBuffer buffer, String value) {
    if (value == null) buffer.putShort((short) -1);
    else {
      var valueByte = value.getBytes(StandardCharsets.UTF_8);
      buffer.putShort((short) valueByte.length);
      buffer.put(ByteBuffer.wrap(valueByte));
    }
  }
}
