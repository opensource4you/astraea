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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker.Std;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;

public interface JsonConverter {

  String toJson(Object src);

  /** for nested generic object ,the return value should specify typeRef , Example: List<String> */
  <T> T fromJson(String json, TypeRef<T> typeRef);

  static JsonConverter defaultConverter() {
    return jackson();
  }

  static <T> JsonDeserializer<T> deserializer(Class<T> clz, Function<String, T> function) {
    return new FromStringDeserializer<>(clz) {
      @Override
      protected T _deserialize(String value, DeserializationContext ctxt) {
        return function.apply(value);
      }
    };
  }

  static <T> JsonSerializer<T> serialzer(Class<T> clz, Function<T, String> function) {
    return new StdScalarSerializer<>(clz) {
      @Override
      public void serialize(T value, JsonGenerator gen, SerializerProvider provider)
          throws IOException {
        gen.writeString(function.apply(value));
      }
    };
  }

  static JsonConverter jackson() {
    var module = new SimpleModule();
    // DataSize
    module.addDeserializer(DataSize.class, deserializer(DataSize.class, DataSize::of));
    module.addSerializer(DataSize.class, serialzer(DataSize.class, DataSize::toString));
    // Duration
    module.addDeserializer(Duration.class, deserializer(Duration.class, Utils::toDuration));
    // TODO: how to support ns?
    // https://github.com/skiptests/astraea/issues/1430
    module.addSerializer(Duration.class, serialzer(Duration.class, d -> d.toMillis() + "ms"));
    var objectMapper =
        JsonMapper.builder()
            .addModule(new Jdk8Module())
            .addModule(module)
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
        || objClass == Object.class
        || objClass == DataSize.class
        || objClass == Duration.class) {
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
