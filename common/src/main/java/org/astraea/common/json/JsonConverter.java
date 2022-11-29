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
package org.astraea.common.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker.Std;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface JsonConverter {

  String toJson(Object src);

  /** for nested generic object ,the return value should specify typeRef , Example: List<String> */
  <T> T fromJson(String json, TypeRef<T> typeRef);

  static JsonConverter defaultConverter() {
    return jackson();
  }

  static JsonConverter jackson() {
    var objectMapper =
        JsonMapper.builder()
            .addModule(new Jdk8Module())
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

            // When we put json as key into kafka, we want to pass the same json to the same
            // destination. When json equals, json needs to be the same string and binary too.
            // So we should satisfy json key order and indentation.
            // sort map
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            // sort object properties
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
            .visibility(new Std(JsonAutoDetect.Visibility.NONE).with(JsonAutoDetect.Visibility.ANY))
            .withConfigOverride(
                List.class,
                o ->
                    o.setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY, Nulls.AS_EMPTY)))
            .withConfigOverride(
                Map.class,
                o ->
                    o.setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY, Nulls.AS_EMPTY)))
            .serializationInclusion(Include.NON_EMPTY)
            .build();

    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        try {
          return objectMapper.writeValueAsString(src);
        } catch (JsonProcessingException e) {
          throw new JsonSerializationException(e);
        }
      }

      @Override
      public <T> T fromJson(String json, TypeRef<T> typeRef) {
        try {
          var t =
              objectMapper.readValue(
                  json,
                  new TypeReference<T>() { // astraea-986 diamond not work (jdk bug)
                    @Override
                    public Type getType() {
                      return typeRef.getType();
                    }
                  });
          preventNull("$", t);
          return t;
        } catch (JsonProcessingException e) {
          throw new JsonSerializationException(e);
        }
      }
    };
  }

  /**
   * User Optional to handle null value.
   *
   * @param name use prefix to locate the error key
   */
  private static void preventNull(String name, Object obj) {
    if (obj == null) throw new JsonSerializationException(name + " can not be null");

    var objClass = obj.getClass();

    if (Collection.class.isAssignableFrom(objClass)) {
      var i = 0;
      for (var c : (Collection<?>) obj) {
        preventNull(name + "[" + i + "]", c);
        i++;
      }
      return;
    }
    if (Optional.class == objClass) {
      var opt = (Optional<?>) obj;
      opt.ifPresent(o -> preventNull(name, o));
      return;
    }
    if (Map.class.isAssignableFrom(objClass)) {
      var map = (Map<?, ?>) obj;
      map.forEach((k, v) -> preventNull(name + "." + k, v));
      return;
    }
    if (objClass.isPrimitive()
        || objClass == Double.class
        || objClass == Float.class
        || objClass == Long.class
        || objClass == Integer.class
        || objClass == Short.class
        || objClass == Character.class
        || objClass == Byte.class
        || objClass == Boolean.class
        || objClass == String.class
        || objClass == Object.class) {
      return;
    }
    Arrays.stream(objClass.getDeclaredFields())
        .peek(x -> x.setAccessible(true))
        .forEach(
            x -> {
              try {
                preventNull(name + "." + x.getName(), x.get(obj));
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
  }
}
