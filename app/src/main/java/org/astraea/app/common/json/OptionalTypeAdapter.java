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
package org.astraea.app.common.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.util.Optional;

/**
 * This is a type adapter for {@code Optional<?>}. Be aware that JSON treat integer and floating
 * value as the same type. So when {@link OptionalTypeAdapter} attempt to deserialize a {@link
 * Optional<Integer>} it always results in {@link Optional<Double>}. To distinguish the difference,
 * consider use {@link java.util.OptionalInt} explicitly in your class object schema.
 */
public class OptionalTypeAdapter
    implements JsonSerializer<Optional<?>>, JsonDeserializer<Optional<?>> {
  @Override
  public JsonElement serialize(Optional<?> src, Type typeOfSrc, JsonSerializationContext context) {
    return src.map(context::serialize).orElse(null);
  }

  @Override
  public Optional<?> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    if (json.isJsonNull()) return Optional.empty();
    return Optional.of(context.deserialize(json, Object.class));
  }
}
