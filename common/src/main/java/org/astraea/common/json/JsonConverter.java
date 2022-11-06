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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker.Std;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.lang.reflect.Type;
import org.astraea.common.Utils;

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
            // sort map keys
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            // sort object properties
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
            .visibility(new Std(JsonAutoDetect.Visibility.NONE).with(JsonAutoDetect.Visibility.ANY))
            .serializationInclusion(Include.NON_EMPTY)
            .build();

    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        return Utils.packException(() -> objectMapper.writeValueAsString(src));
      }

      @Override
      public <T> T fromJson(String json, TypeRef<T> typeRef) {
        return Utils.packException(
            () ->
                objectMapper.readValue(
                    json,
                    new TypeReference<T>() { // astraea-986 diamond not work (jdk bug)
                      @Override
                      public Type getType() {
                        return typeRef.getType();
                      }
                    }));
      }
    };
  }
}
