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
package org.astraea.app.common;

import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.reflections.Reflections;

class EnumInfoTest {

  @Test
  void testAlias() {
    Assertions.assertEquals("TEST", MyTestEnum.TEST.alias());
    Assertions.assertEquals(MyTestEnum.TEST, MyTestEnum.ofAlias("test"));
  }

  @ParameterizedTest
  @ArgumentsSource(EnumClassProvider.class)
  void testExtendEnumInfo(Class<?> cls) {
    Assertions.assertTrue(
        EnumInfo.class.isAssignableFrom(cls), String.format("Fail class %s", cls));
  }

  @ParameterizedTest
  @ArgumentsSource(EnumClassProvider.class)
  void testOfAlias(Class<?> cls) {
    // some enum implement anonymous class , see DistributionType
    var enumCls = getClassParentIsEnum(cls);

    var method =
        Assertions.assertDoesNotThrow(
            () -> enumCls.getDeclaredMethod("ofAlias", String.class),
            String.format("Fail class %s", cls));
    Assertions.assertEquals(enumCls, method.getReturnType());
  }

  private Class<?> getClassParentIsEnum(Class<?> cls) {
    return cls.getSuperclass() == Enum.class ? cls : getClassParentIsEnum(cls.getSuperclass());
  }

  enum MyTestEnum implements EnumInfo {
    TEST;

    public static MyTestEnum ofAlias(String alias) {
      return Utils.ignoreCaseEnum(MyTestEnum.class, alias);
    }
  }

  public static class EnumClassProvider implements ArgumentsProvider {

    private final Reflections reflections = new Reflections("org.astraea.app");

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return reflections.getSubTypesOf(Enum.class).stream().map(Arguments::of);
    }
  }
}
