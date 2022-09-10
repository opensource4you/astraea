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
package org.astraea.common;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** TODO: 2022-09-10 astraea#694 The test is duplicated in common module. Can we avoid it? */
class EnumInfoTest {

  @Test
  void testAlias() {
    Assertions.assertEquals("TEST", MyTestEnum.TEST.alias());
    Assertions.assertEquals(MyTestEnum.TEST, MyTestEnum.ofAlias("test"));

    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> MyTestEnum.ofAlias("NotInEnum"));
    Assertions.assertTrue(exception.getMessage().contains("NotInEnum"));
  }

  @Test
  void testIgnoreCaseEnum() {
    Assertions.assertEquals(MyTestEnum.TEST, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "test"));
    Assertions.assertEquals(MyTestEnum.TEST, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "TEST"));
    Assertions.assertEquals(MyTestEnum.TEST, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "Test"));
    Assertions.assertEquals(MyTestEnum.BANANA, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "Banana"));
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
    var method =
        Assertions.assertDoesNotThrow(
            () -> cls.getDeclaredMethod("ofAlias", String.class),
            String.format("Fail class %s", cls));
    Assertions.assertEquals(cls, method.getReturnType());
  }

  @ParameterizedTest
  @ArgumentsSource(EnumClassProvider.class)
  void testToString(Class<?> cls) {
    var enumConstants = (EnumInfo[]) cls.getEnumConstants();
    Assertions.assertTrue(
        Arrays.stream(enumConstants).allMatch(x -> x.toString().equals(x.alias())));
  }

  enum MyTestEnum implements EnumInfo {
    TEST,
    BANANA;

    public static MyTestEnum ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(MyTestEnum.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }
  }

  public static class EnumClassProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return getProductionClass().stream().filter(Class::isEnum).map(Arguments::of);
    }
  }

  @Test
  void testProductionClass() {
    var productionClasses = getProductionClass();
    Assertions.assertTrue(productionClasses.size() > 100);
    Assertions.assertTrue(
        productionClasses.stream().allMatch(x -> x.getPackageName().startsWith("org.astraea")));
    System.out.println(
        productionClasses.stream().filter(Class::isEnum).collect(Collectors.toList()));
  }

  @Test
  void testEnumClassProvider() {
    var enumClassProvider = new EnumClassProvider();
    var enumCls = enumClassProvider.provideArguments(null).collect(Collectors.toList());
    Assertions.assertTrue(enumCls.size() > 0);
    Assertions.assertTrue(enumCls.stream().map(x -> (Class<?>) x.get()[0]).allMatch(Class::isEnum));
  }

  private static List<Class<?>> getProductionClass() {
    var pkg = "org/astraea";
    System.out.println(EnumInfoTest.class.getClassLoader());
    var mainDir =
        Collections.list(
                Utils.packException(() -> EnumInfoTest.class.getClassLoader().getResources(pkg)))
            .stream()
            .peek(x -> System.out.println(x.toExternalForm()))
            .filter(x -> x.toExternalForm().contains("main/" + pkg))
            .findFirst()
            .map(x -> Utils.packException(() -> Path.of(x.toURI())))
            .map(x -> x.resolve("../../").normalize())
            .orElseThrow();

    var dirFiles =
        FileUtils.listFiles(mainDir.toFile(), new String[] {"class"}, true).stream()
            .map(File::toPath)
            .map(mainDir::relativize)
            .collect(Collectors.toList());

    var classNames =
        dirFiles.stream()
            .map(Path::toString)
            .map(FilenameUtils::removeExtension)
            .map(x -> x.replace(File.separatorChar, '.'))
            .collect(Collectors.toList());

    return classNames.stream()
        .map(x -> Utils.packException(() -> Class.forName(x)))
        .collect(Collectors.toList());
  }
}
