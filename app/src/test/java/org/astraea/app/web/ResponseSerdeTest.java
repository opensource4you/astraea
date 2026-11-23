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
package org.astraea.app.web;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.astraea.app.TestUtils;
import org.astraea.app.web.RecordHandler.PostResponse;
import org.astraea.common.DataSize;
import org.astraea.common.json.JsonConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class ResponseSerdeTest {

  @ParameterizedTest
  @ArgumentsSource(ResponseClassProvider.class)
  void testResponseImpl(Class<?> cls) throws Exception {
    validateJsonSerde(cls);
  }

  @Test
  void testListResponses() {
    var classes = responseClasses();
    Assertions.assertNotEquals(0, classes.size());
    Assertions.assertTrue(classes.contains(PostResponse.class));
  }

  private static List<Class<?>> responseClasses() {
    return TestUtils.getProductionClass().stream()
        .filter(Response.class::isAssignableFrom)
        .filter(c -> !c.isInterface())
        .toList();
  }

  public static class ResponseClassProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return responseClasses().stream().map(Arguments::of);
    }
  }

  private void validateJsonSerde(Class<?> cls) throws Exception {
    for (var field : cls.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers()))
        continue;
      field.setAccessible(true);
      var type = field.getType();
      var jsonFriendly =
          Optional.class == type
              || Map.class.isAssignableFrom(type)
              || Collection.class.isAssignableFrom(type)
              || type.isPrimitive()
              || type.isEnum()
              || type == Double.class
              || type == Float.class
              || type == Long.class
              || type == Integer.class
              || type == Short.class
              || type == Character.class
              || type == Byte.class
              || type == Boolean.class
              || type == String.class
              || type == Object.class
              // types with a custom serializer in JsonConverter
              || type == DataSize.class
              || type == Duration.class
              || Request.class.isAssignableFrom(type)
              || Response.class.isAssignableFrom(type);
      Assertions.assertTrue(
          jsonFriendly,
          String.format(
              "the field %s of %s is neither a JSON-friendly type nor a Request/Response",
              field.getName(), cls));
    }

    // a no-arg constructor means the type can also be deserialized, so serializing a fresh
    // instance must not fail
    var ctor = noArgConstructor(cls);
    if (ctor.isEmpty()) return;
    ctor.get().setAccessible(true);
    var instance = ctor.get().newInstance();
    var converter = JsonConverter.defaultConverter();
    Assertions.assertDoesNotThrow(() -> converter.toJson(instance));
  }

  private static Optional<Constructor<?>> noArgConstructor(Class<?> cls) {
    try {
      return Optional.of(cls.getDeclaredConstructor());
    } catch (NoSuchMethodException e) {
      return Optional.empty();
    }
  }
}
