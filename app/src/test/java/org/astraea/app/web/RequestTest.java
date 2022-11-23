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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.web.RecordHandler.PostRecord;
import org.astraea.app.web.RecordHandler.RecordPostRequest;
import org.astraea.app.web.Request.RequestObject;
import org.astraea.common.Utils;
import org.astraea.web.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class RequestTest {

  @ParameterizedTest
  @ArgumentsSource(RequestClassProvider.class)
  void testRequestImpl(Class<?> cls) {
    validateDefaultConstructor(cls);
    validateDefaultAssign(cls);
  }

  @Test
  void testRequestClassProvider() {
    var classes =
        new RequestClassProvider()
            .provideArguments(null)
            .map(x -> (Class<?>) x.get()[0])
            .collect(Collectors.toList());

    Assertions.assertTrue(classes.contains(RecordPostRequest.class));
    Assertions.assertTrue(classes.contains(PostRecord.class));
  }

  @Test
  void testValidateConstructor() {
    Assertions.assertThrows(
        Throwable.class, () -> validateDefaultConstructor(NoDefaultConstructor.class));

    validateDefaultConstructor(NoDefaultAssignMap.class);
  }

  @Test
  void testValidateAssign() {
    Assertions.assertThrows(
        Throwable.class, () -> validateDefaultAssign(NoDefaultAssignList.class));
    Assertions.assertThrows(
        Throwable.class, () -> validateDefaultAssign(NoDefaultAssignOptional.class));
    Assertions.assertThrows(Throwable.class, () -> validateDefaultAssign(NoDefaultAssignMap.class));

    validateDefaultAssign(HasDefaultAssignList.class);
  }

  private void validateDefaultConstructor(Class<?> cls) {
    Assertions.assertDoesNotThrow(
        () -> {
          cls.getDeclaredConstructor();
        });
  }

  /** test optional, map, list assigned */
  private void validateDefaultAssign(Class<?> cls) {
    var instance = Utils.packException(() -> cls.getDeclaredConstructor().newInstance());
    Arrays.stream(cls.getDeclaredFields())
        .peek(x -> x.setAccessible(true))
        .forEach(
            x ->
                Utils.packException(
                    () -> {
                      var innerCls = x.getType();
                      if (Optional.class == innerCls
                          || Map.class.isAssignableFrom(innerCls)
                          || List.class.isAssignableFrom(innerCls)) {
                        Assertions.assertNotNull(x.get(instance));
                      }
                    }));
  }

  public static class RequestClassProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return TestUtils.getProductionClass().stream()
          .filter(x -> Request.class.isAssignableFrom(x) || RequestObject.class.isAssignableFrom(x))
          .map(RequestTest::getAllFieldPojoCls)
          .flatMap(Collection::stream)
          .map(Arguments::of);
    }
  }

  public static List<Class<?>> getAllFieldPojoCls(Class<?> cls) {
    if (isPojo(cls)) {
      return Stream.concat(
              Stream.of(cls),
              Arrays.stream(cls.getDeclaredFields())
                  .peek(x -> System.out.println(x.getName()))
                  .map(Field::getType)
                  .flatMap(x -> getAllFieldPojoCls(x).stream()))
          .collect(Collectors.toList());
    } else {
      return List.of();
    }
  }

  static class NoDefaultConstructor {
    String a;

    public NoDefaultConstructor(String a) {
      this.a = a;
    }
  }

  static class NoDefaultAssignMap {
    Map<String, String> map;
  }

  static class NoDefaultAssignList {
    List<String> list;
  }

  static class NoDefaultAssignOptional {
    Optional<String> opt;
  }

  static class HasDefaultAssignList {
    List<String> list = List.of();
  }

  public static boolean isPojo(Class<?> cls) {
    return !(cls.isPrimitive()
        || Utils.isWrapper(cls)
        || cls.isSynthetic()
        || cls.isInterface()
        || Collection.class.isAssignableFrom(cls)
        || Map.class.isAssignableFrom(cls)
        || String.class == cls
        || Optional.class == cls
        || Object.class == cls);
  }
}
