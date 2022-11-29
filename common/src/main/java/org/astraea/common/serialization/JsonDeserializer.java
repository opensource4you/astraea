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

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public class JsonDeserializer<T> implements Deserializer<T> {
  private final String TYPE = "deserializer.type";
  private final TypeRef<T> typeRef;
  private final String encoding = StandardCharsets.UTF_8.name();
  private final JsonConverter jackson = JsonConverter.jackson();

  public static <T> JsonDeserializer<T> of(TypeRef<T> typeRef) {
    Type type = typeRef.getType();
    return new JsonDeserializer<>(typeRef);
  }

  private JsonDeserializer(TypeRef<T> typeRef) {
    this.typeRef = typeRef;
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) return null;
    else {
      return jackson.fromJson(Utils.packException(() -> new String(data, encoding)), typeRef);
    }
  }
}
