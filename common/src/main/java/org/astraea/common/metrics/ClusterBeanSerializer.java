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
package org.astraea.common.metrics;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;

public class ClusterBeanSerializer {

  public static void serialize(ClusterBean clusterBean, OutputStream stream) {
    Utils.packException(
        () -> {
          var buffer = ByteBuffer.allocate(64 << 20); // 64 MB buffer
          var rawMap = clusterBean.all();

          // put size
          buffer.putInt(rawMap.size());
          rawMap.forEach(
              (broker, metrics) -> {
                // put id
                buffer.putInt(broker);

                // put metrics size
                buffer.putInt(metrics.size());

                // put metrics
                metrics.forEach(
                    beanObject -> {
                      var object = beanObject.beanObject();
                      // put type name
                      buffer.putInt(beanObject.getClass().getName().length());
                      buffer.put(beanObject.getClass().getName().getBytes());

                      // put domain name size
                      buffer.putInt(object.domainName().length());
                      // put domain name
                      buffer.put(object.domainName().getBytes());

                      // put property size
                      buffer.putInt(object.properties().size());
                      // put properties
                      object
                          .properties()
                          .forEach(
                              (k, v) -> {
                                buffer.putInt(k.length());
                                buffer.put(k.getBytes());
                                buffer.putInt(v.length());
                                buffer.put(v.getBytes());
                              });

                      // put attribute size
                      buffer.putInt(object.attributes().size());
                      // put attributes
                      object
                          .attributes()
                          .forEach(
                              (k, v) -> {
                                // put attribute key
                                buffer.putInt(k.length());
                                buffer.put(k.getBytes());

                                // put attribute
                                AttributeType type = AttributeType.from(v);
                                buffer.putInt(type.value);
                                switch (type) {
                                  case Integer:
                                    buffer.putInt((int) v);
                                    break;
                                  case Long:
                                    buffer.putLong((long) v);
                                    break;
                                  case Double:
                                    buffer.putDouble((double) v);
                                    break;
                                  case String:
                                    buffer.putInt(((String) v).length());
                                    buffer.put(((String) v).getBytes());
                                    break;
                                  case Boolean:
                                    buffer.putInt(((Boolean) v ? 1 : 0));
                                    break;
                                  default:
                                    System.out.println(
                                        "Doesn't support this type "
                                            + k
                                            + "="
                                            + v.getClass().getName());
                                    break;
                                }
                              });
                    });
              });
          stream.write(buffer.array(), 0, buffer.position());
        });
  }

  public static ClusterBean deserialize(InputStream stream) {
    var metrics = new HashMap<Integer, List<HasBeanObject>>();
    var buffer = ByteBuffer.wrap(Utils.packException(stream::readAllBytes));

    for (int i = 0, len = buffer.getInt(); i < len; i++) {
      // get broker id
      var brokerId = buffer.getInt();
      // get metric size
      var metricSize = buffer.getInt();

      metrics.put(brokerId, new ArrayList<>());

      for (int j = 0; j < metricSize; j++) {
        // get type name
        var typeName = strFromBuffer(buffer);
        // get domain name
        var domain = strFromBuffer(buffer);
        var properties = new HashMap<String, String>();
        var attributes = new HashMap<String, Object>();
        // get property size
        var propertySize = buffer.getInt();
        for (int k = 0; k < propertySize; k++) {
          var key = strFromBuffer(buffer);
          var value = strFromBuffer(buffer);
          properties.put(key, value);
        }

        // get attribute size
        var attributeSize = buffer.getInt();
        for (int k = 0; k < attributeSize; k++) {
          var key = strFromBuffer(buffer);
          var attributeType = AttributeType.from(buffer.getInt());
          var value = (Object) null;

          switch (attributeType) {
            case Integer:
              value = buffer.getInt();
              break;
            case Long:
              value = buffer.getLong();
              break;
            case Double:
              value = buffer.getDouble();
              break;
            case String:
              value = strFromBuffer(buffer);
              break;
            case Boolean:
              value = buffer.getInt() != 0;
              break;
            default:
              value = null;
              break;
          }

          attributes.put(key, value);
        }

        // construct metric
        var metric =
            (HasBeanObject)
                Utils.packException(
                    () ->
                        Class.forName(typeName)
                            .getConstructor(BeanObject.class)
                            .newInstance(new BeanObject(domain, properties, attributes)));
        metrics.get(brokerId).add(metric);
      }
    }

    return ClusterBean.of(metrics);
  }

  private static String strFromBuffer(ByteBuffer buffer) {
    var size = buffer.getInt();
    var bytes = new byte[size];
    for (int i = 0; i < size; i++) bytes[i] = buffer.get();
    return new String(bytes);
  }

  enum AttributeType {
    Unknown(-1),
    Integer(1),
    Long(2),
    Double(3),
    String(4),
    Boolean(5);

    private static final List<AttributeType> attributes = List.of(AttributeType.values());

    public final int value;

    AttributeType(int value) {
      this.value = value;
    }

    static AttributeType from(Object object) {
      if (object instanceof Integer) return Integer;
      else if (object instanceof Long) return Long;
      else if (object instanceof Double) return Double;
      else if (object instanceof String) return String;
      else if (object instanceof Boolean) return Boolean;
      else return Unknown;
    }

    static AttributeType from(int i) {
      return attributes.stream().filter(x -> x.value == i).findFirst().orElseThrow();
    }
  }
}
