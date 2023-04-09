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
package org.astraea.common.serialization;

import org.astraea.common.generated.BeanObjectOuterClass;
import org.astraea.common.generated.PrimitiveOuterClass;
import org.astraea.common.metrics.BeanObject;

public interface Serializer<T> {
  byte[] serialize(T data);

  /** Serialize BeanObject by protocol buffer. */
  Serializer<BeanObject> BEAN_OBJECT_SERIALIZER =
      beanObject -> {
        var beanBuilder = BeanObjectOuterClass.BeanObject.newBuilder();
        beanBuilder.setDomain(beanObject.domainName());
        beanBuilder.putAllProperties(beanObject.properties());
        beanObject
            .attributes()
            .forEach((key, val) -> beanBuilder.putAttributes(key, primitive(val)));
        return beanBuilder.build().toByteArray();
      };

  /** Convert java primitive type to "one of" protocol buffer primitive type. */
  static PrimitiveOuterClass.Primitive primitive(Object v) {
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
      throw new IllegalArgumentException(
          "Type "
              + v.getClass()
              + " is not supported. Please use Integer, Long, Float, Double, Boolean, String instead.");
  }
}
