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
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker.Std;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Base64;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import org.astraea.common.Utils;

public interface JsonConverter {

  String toJson(Object src);

  <T> T fromJson(String json, Class<T> tClass);

  /** for nested generic object ,the return value should specify typeRef , Example: List<String> */
  <T> T fromJson(String json, TypeRef<T> typeRef);

  static JsonConverter defaultConverter() {
    return gson();
  }

  static JsonConverter jackson() {
    var objectMapper =
        JsonMapper.builder()
            .addModule(new Jdk8Module())
            //            .constructorDetector(ConstructorDetector.USE_DELEGATING)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
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
      public <T> T fromJson(String json, Class<T> tClass) {
        return Utils.packException(() -> objectMapper.readValue(json, tClass));
      }

      @Override
      public <T> T fromJson(String json, TypeRef<T> typeRef) {
        return Utils.packException(
            () ->
                objectMapper.readValue(
                    json,
                    new TypeReference<>() {
                      @Override
                      public Type getType() {
                        return typeRef.getType();
                      }
                    }));
      }
    };
  }

  static JsonConverter gson() {
    return gson((builder) -> {});
  }

  static JsonConverter gson(Consumer<GsonBuilder> builderConsumer) {
    var gsonBuilder =
        new GsonBuilder()
            //        .registerTypeAdapter(Optional.class,new GsonOptionalDeserializer<>())

            .disableHtmlEscaping()
            .registerTypeAdapterFactory(OptionalTypeAdapter.FACTORY)
            .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter());

    builderConsumer.accept(gsonBuilder);
    var gson = gsonBuilder.create();

    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        var jsonElement = gson.toJsonTree(src);
        return gson.toJson(getOrderedObject(jsonElement));
      }

      /**
       * Gson does not support name strategy, but the field order of serialization is the order in
       * JsonElement.
       */
      private JsonElement getOrderedObject(JsonElement jsonElement) {
        if (jsonElement.isJsonObject()) {
          return getOrderedObject(jsonElement.getAsJsonObject());
        } else {
          return jsonElement;
        }
      }

      private JsonObject getOrderedObject(JsonObject jsonObject) {
        var newJsonObject = new JsonObject();
        jsonObject.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .forEach(x -> newJsonObject.add(x.getKey(), getOrderedObject(x.getValue())));
        return newJsonObject;
      }

      @Override
      public <T> T fromJson(String json, Class<T> tClass) {
        return gson.fromJson(json, tClass);
      }

      @Override
      public <T> T fromJson(String json, TypeRef<T> typeRef) {
        return gson.fromJson(json, typeRef.getType());
      }
    };
  }

  class ByteArrayToBase64TypeAdapter implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
    public byte[] deserialize(JsonElement json, Type type, JsonDeserializationContext context)
        throws JsonParseException {
      return Base64.getDecoder().decode(json.getAsString());
    }

    public JsonElement serialize(byte[] src, Type type, JsonSerializationContext context) {
      return new JsonPrimitive(Base64.getEncoder().encodeToString(src));
    }
  }

  /**
   * Gson use ReflectiveTypeAdapterFactory to serde the Object class. When the field key is not in
   * json, ReflectiveTypeAdapterFactory didn't pass it forward to OptionalTypeAdapter. So we should
   * init Optional by Optional opt=Optional.empty() in Object. This didn't work when no default
   * constructor to init Optional to empty()
   *
   * <p>OptionalTypeAdapter also can't be replaced by `JsonDeserializer`, because JsonDeserializer
   * didn't get value when value is null in json.
   */
  class OptionalTypeAdapter<E> extends TypeAdapter<Optional<E>> {

    public static final TypeAdapterFactory FACTORY =
        new TypeAdapterFactory() {
          @Override
          public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            Class<T> rawType = (Class<T>) type.getRawType();
            if (rawType != Optional.class) {
              return null;
            }
            final ParameterizedType parameterizedType = (ParameterizedType) type.getType();
            final Type actualType = parameterizedType.getActualTypeArguments()[0];
            final TypeAdapter<?> adapter = gson.getAdapter(TypeToken.get(actualType));
            return new OptionalTypeAdapter(adapter);
          }
        };
    private final TypeAdapter<E> adapter;

    public OptionalTypeAdapter(TypeAdapter<E> adapter) {

      this.adapter = adapter;
    }

    @Override
    public void write(JsonWriter out, Optional<E> value) throws IOException {
      if (value.isPresent()) {
        adapter.write(out, value.get());
      } else {
        out.nullValue();
      }
    }

    @Override
    public Optional<E> read(JsonReader in) throws IOException {
      final JsonToken peek = in.peek();
      if (peek != JsonToken.NULL) {
        return Optional.ofNullable(adapter.read(in));
      }

      in.nextNull();
      return Optional.empty();
    }
  }

  class GsonOptionalDeserializer<T>
      implements JsonSerializer<Optional<T>>, JsonDeserializer<Optional<T>> {

    @Override
    public Optional<T> deserialize(
        JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      if (!json.isJsonNull()) {
        final T value =
            context.deserialize(json, ((ParameterizedType) typeOfT).getActualTypeArguments()[0]);
        return Optional.ofNullable(value);
      } else {
        return Optional.empty();
      }
    }

    @Override
    public JsonElement serialize(
        Optional<T> src, Type typeOfSrc, JsonSerializationContext context) {
      if (src.isPresent()) {
        return context.serialize(src.get());
      } else {
        return JsonNull.INSTANCE;
      }
    }
  }
}
