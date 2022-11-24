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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.web.RecordHandler.PostRecord;
import org.astraea.app.web.RecordHandler.RecordPostRequest;
import org.astraea.web.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opentest4j.AssertionFailedError;

class RequestTest {

  @ParameterizedTest
  @ArgumentsSource(RequestClassProvider.class)
  void testRequestImpl(Class<?> cls) throws Exception {
    validateDefaultAssign(cls);
  }

  @Test
  void testListRequests() {
    var classes = requestClasses();
    Assertions.assertNotEquals(0, requestClasses().size());
    Assertions.assertTrue(classes.contains(RecordPostRequest.class));
    Assertions.assertTrue(classes.contains(PostRecord.class));
  }

  @Test
  void testNestedNonRequest() {
    // C0 does not implement Request
    Assertions.assertThrows(AssertionFailedError.class, () -> validateDefaultAssign(C1.class));
  }

  private static List<Class<?>> requestClasses() {
    return TestUtils.getProductionClass().stream()
        .filter(Request.class::isAssignableFrom)
        .filter(c -> !c.isInterface())
        .collect(Collectors.toList());
  }

  public static class RequestClassProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return requestClasses().stream().map(Arguments::of);
    }
  }

  private void validateDefaultAssign(Class<?> cls) throws Exception {
    // 1) Each request should have default constructor.
    var instance = cls.getDeclaredConstructor().newInstance();
    for (var x : cls.getDeclaredFields()) {
      x.setAccessible(true);
      var innerCls = x.getType();
      // 2) Optional, Collection and Map field should be initialized
      if (Optional.class == innerCls
          || Map.class.isAssignableFrom(innerCls)
          || Collection.class.isAssignableFrom(innerCls)) {
        Assertions.assertNotNull(x.get(instance));
        return;
      }
      if (innerCls.isPrimitive()
          || innerCls == Double.class
          || innerCls == Float.class
          || innerCls == Long.class
          || innerCls == Integer.class
          || innerCls == Short.class
          || innerCls == Character.class
          || innerCls == Byte.class
          || innerCls == Boolean.class
          || innerCls == String.class
          || innerCls == Object.class) {
        return;
      }
      // The other allowed type is only `Request`
      Assertions.assertEquals(Request.class, innerCls);
    }
  }

  private static class C0 {
    Integer a;
  }

  private static class C1 implements Request {
    C0 c0;
  }
}
