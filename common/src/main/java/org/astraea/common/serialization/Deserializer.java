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

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.generated.BeanObjectOuterClass;
import org.astraea.common.generated.PrimitiveOuterClass;
import org.astraea.common.metrics.BeanObject;

public interface Deserializer<T> {
  T deserialize(byte[] data);

  /** Deserialize to BeanObject with protocol buffer */
  Deserializer<BeanObject> BEAN_OBJECT =
      data -> {
        // Pack InvalidProtocolBufferException thrown by protoBuf
        var outerBean = Utils.packException(() -> BeanObjectOuterClass.BeanObject.parseFrom(data));
        return new BeanObject(
            outerBean.getDomain(),
            outerBean.getPropertiesMap(),
            outerBean.getAttributesMap().entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, e -> Objects.requireNonNull(toObject(e.getValue())))));
      };

  /** Retrieve field from "one of" field. */
  static Object toObject(PrimitiveOuterClass.Primitive v) {
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
