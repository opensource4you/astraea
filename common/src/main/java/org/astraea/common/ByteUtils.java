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
import java.util.Collection;
import java.util.List;
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
import org.astraea.common.admin.TopicPartitionPath;
import org.astraea.common.generated.BeanObjectOuterClass;
import org.astraea.common.generated.PrimitiveOuterClass;
import org.astraea.common.generated.admin.BrokerOuterClass;
import org.astraea.common.generated.admin.ClusterInfoOuterClass;
import org.astraea.common.generated.admin.ReplicaOuterClass;
import org.astraea.common.generated.admin.TopicOuterClass;
import org.astraea.common.generated.admin.TopicPartitionOuterClass;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;

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
    return toOuterClass(value).toByteArray();
  }

  /** Serialize ClusterInfo by protocol buffer. */
  public static byte[] toBytes(ClusterInfo value) {
    return ClusterInfoOuterClass.ClusterInfo.newBuilder()
        .setClusterId(value.clusterId())
        .addAllBroker(value.brokers().stream().map(ByteUtils::toOuterClass).toList())
        .addAllTopic(value.topics().values().stream().map(ByteUtils::toOuterClass).toList())
        .addAllReplica(value.replicas().stream().map(ByteUtils::toOuterClass).toList())
        .build()
        .toByteArray();
  }

  public static byte[] toBytes(Map<Integer, Collection<HasBeanObject>> values) {
    var mapOfBeanObjects =
        values.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(HasBeanObject::beanObject)
                            // convert BeanObject to protocol buffer
                            .map(ByteUtils::toOuterClass)
                            .toList()));

    return BeanObjectOuterClass.MapOfBeanObjects.newBuilder()
        .putAllAllBeans(
            mapOfBeanObjects.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        Objects ->
                            BeanObjectOuterClass.MapOfBeanObjects.BeanObjects.newBuilder()
                                .addAllBeanObjects(Objects.getValue())
                                .build())))
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

  /** Deserialize to a map with Integer keys and list of BeanObject values using protocol buffer */
  public static Map<Integer, List<BeanObject>> readBeanObjects(byte[] bytes) {
    try {
      var outerClusterBean = BeanObjectOuterClass.MapOfBeanObjects.parseFrom(bytes);
      return outerClusterBean.getAllBeansMap().entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  k -> k.getKey(),
                  v ->
                      v.getValue().getBeanObjectsList().stream()
                          .map(
                              i ->
                                  new BeanObject(
                                      i.getDomain(),
                                      i.getPropertiesMap(),
                                      i.getAttributesMap().entrySet().stream()
                                          .collect(
                                              Collectors.toUnmodifiableMap(
                                                  Map.Entry::getKey,
                                                  e ->
                                                      Objects.requireNonNull(
                                                          toObject(e.getValue())))),
                                      i.getCreatedTimestamp().getSeconds() * 1000
                                          + i.getCreatedTimestamp().getNanos() / 1000000))
                          .toList()));
    } catch (InvalidProtocolBufferException ex) {
      throw new SerializationException(ex);
    }
  }

  /** Deserialize to ClusterInfo with protocol buffer */
  public static ClusterInfo readClusterInfo(byte[] bytes) {
    try {
      var outerClusterInfo = ClusterInfoOuterClass.ClusterInfo.parseFrom(bytes);
      return ClusterInfo.of(
          outerClusterInfo.getClusterId(),
          outerClusterInfo.getBrokerList().stream().map(ByteUtils::toBroker).toList(),
          outerClusterInfo.getTopicList().stream()
              .map(ByteUtils::toTopic)
              .collect(Collectors.toMap(Topic::name, Function.identity())),
          outerClusterInfo.getReplicaList().stream().map(ByteUtils::toReplica).toList());
    } catch (InvalidProtocolBufferException ex) {
      throw new SerializationException(ex);
    }
  }

  // ---------------------------Serialize To ProtoBuf Outer Class------------------------------- //

  private static BrokerOuterClass.Broker.TopicPartitionPath toOuterClass(
      TopicPartitionPath topicPartitionPath) {
    return BrokerOuterClass.Broker.TopicPartitionPath.newBuilder()
        .setTopic(topicPartitionPath.topic())
        .setPartition(topicPartitionPath.partition())
        .setSize(topicPartitionPath.size())
        .setPath(topicPartitionPath.path())
        .build();
  }

  private static TopicPartitionOuterClass.TopicPartition toOuterClass(
      TopicPartition topicPartition) {
    return TopicPartitionOuterClass.TopicPartition.newBuilder()
        .setPartition(topicPartition.partition())
        .setTopic(topicPartition.topic())
        .build();
  }

  private static BrokerOuterClass.Broker toOuterClass(Broker broker) {
    return BrokerOuterClass.Broker.newBuilder()
        .setId(broker.id())
        .setHost(broker.host())
        .setPort(broker.port())
        .setIsController(broker.isController())
        .putAllConfig(broker.config().raw())
        .addAllDataFolders(broker.dataFolders())
        .addAllTopicPartitionPaths(
            broker.topicPartitionPaths().stream().map(ByteUtils::toOuterClass).toList())
        .build();
  }

  private static TopicOuterClass.Topic toOuterClass(Topic topic) {
    return TopicOuterClass.Topic.newBuilder()
        .setName(topic.name())
        .putAllConfig(topic.config().raw())
        .setInternal(topic.internal())
        .addAllPartitionIds(topic.partitionIds())
        .build();
  }

  private static ReplicaOuterClass.Replica toOuterClass(Replica replica) {
    return ReplicaOuterClass.Replica.newBuilder()
        .setTopic(replica.topic())
        .setPartition(replica.partition())
        .setBrokerId(replica.brokerId())
        .setLag(replica.lag())
        .setSize(replica.size())
        .setIsInternal(replica.isInternal())
        .setIsLeader(replica.isLeader())
        .setIsAdding(replica.isAdding())
        .setIsRemoving(replica.isRemoving())
        .setIsSync(replica.isSync())
        .setIsFuture(replica.isFuture())
        .setIsOffline(replica.isOffline())
        .setIsPreferredLeader(replica.isPreferredLeader())
        .setPath(replica.path())
        .build();
  }

  private static BeanObjectOuterClass.BeanObject toOuterClass(BeanObject beanObject) {
    var beanBuilder = BeanObjectOuterClass.BeanObject.newBuilder();
    beanBuilder.setDomain(beanObject.domainName());
    beanBuilder.putAllProperties(beanObject.properties());
    beanObject
        .attributes()
        .forEach(
            (key, val) -> {
              try {
                beanBuilder.putAttributes(key, primitive(val));
              } catch (SerializationException ignore) {
                // Bean attribute may contain non-primitive value. e.g. TimeUnit, Byte.
              }
            });
    return beanBuilder
        // the following code sets the created timestamp field using
        // the recommended
        // style by protobuf documentation.
        .setCreatedTimestamp(
            Timestamp.newBuilder()
                .setSeconds(beanObject.createdTimestamp() / 1000)
                .setNanos((int) (beanObject.createdTimestamp() % 1000 * 1000000)))
        .build();
  }

  // -------------------------Deserialize From ProtoBuf Outer Class----------------------------- //

  private static TopicPartitionPath toTopicPartitionPath(
      BrokerOuterClass.Broker.TopicPartitionPath partitionPath) {
    return new TopicPartitionPath(
        partitionPath.getTopic(),
        partitionPath.getPartition(),
        partitionPath.getSize(),
        partitionPath.getPath());
  }

  private static TopicPartition toTopicPartition(
      TopicPartitionOuterClass.TopicPartition topicPartition) {
    return new TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
  }

  private static Broker toBroker(BrokerOuterClass.Broker broker) {
    return new Broker(
        broker.getId(),
        broker.getHost(),
        broker.getPort(),
        broker.getIsController(),
        new Config(broker.getConfigMap()),
        Set.copyOf(broker.getDataFoldersList()),
        broker.getTopicPartitionPathsList().stream().map(ByteUtils::toTopicPartitionPath).toList());
  }

  private static Topic toTopic(TopicOuterClass.Topic topic) {
    return new Topic(
        topic.getName(),
        new Config(topic.getConfigMap()),
        topic.getInternal(),
        Set.copyOf(topic.getPartitionIdsList()));
  }

  private static Replica toReplica(ReplicaOuterClass.Replica replica) {
    return Replica.builder()
        .topic(replica.getTopic())
        .partition(replica.getPartition())
        .brokerId(replica.getBrokerId())
        .lag(replica.getLag())
        .size(replica.getSize())
        .isInternal(replica.getIsInternal())
        .isLeader(replica.getIsLeader())
        .isAdding(replica.getIsAdding())
        .isRemoving(replica.getIsRemoving())
        .isSync(replica.getIsSync())
        .isFuture(replica.getIsFuture())
        .isOffline(replica.getIsOffline())
        .isPreferredLeader(replica.getIsPreferredLeader())
        .path(replica.getPath())
        .build();
  }

  // --------------------------------ProtoBuf Primitive----------------------------------------- //

  /**
   * Convert java primitive type to "one of" protocol buffer primitive type. There are no "short"
   * and "char" in Protocol Buffers. Use "int" and "String" instead.
   */
  private static PrimitiveOuterClass.Primitive primitive(Object v) throws SerializationException {
    if (v instanceof Integer value)
      return PrimitiveOuterClass.Primitive.newBuilder().setInt(value).build();
    else if (v instanceof Long value)
      return PrimitiveOuterClass.Primitive.newBuilder().setLong(value).build();
    else if (v instanceof Float value)
      return PrimitiveOuterClass.Primitive.newBuilder().setFloat(value).build();
    else if (v instanceof Double value)
      return PrimitiveOuterClass.Primitive.newBuilder().setDouble(value).build();
    else if (v instanceof Boolean value)
      return PrimitiveOuterClass.Primitive.newBuilder().setBoolean(value).build();
    else if (v instanceof String value)
      return PrimitiveOuterClass.Primitive.newBuilder().setStr(value).build();
    else
      throw new SerializationException(
          "Type "
              + v.getClass()
              + " is not supported. Please use Integer, Long, Float, Double, Boolean, String instead.");
  }

  /** Retrieve field from "one of" field. */
  private static Object toObject(PrimitiveOuterClass.Primitive v) {
    var oneOfCase = v.getValueCase();
    return switch (oneOfCase) {
      case INT -> v.getInt();
      case LONG -> v.getLong();
      case FLOAT -> v.getFloat();
      case DOUBLE -> v.getDouble();
      case BOOLEAN -> v.getBoolean();
      case STR -> v.getStr();
      default -> throw new IllegalArgumentException("The value is not set.");
    };
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
