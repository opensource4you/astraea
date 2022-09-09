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
package org.astraea.common.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PositiveNumberListFieldTest {
  private static class FakeIntegerParameter {
    @Parameter(
        names = {"--Ifield"},
        validateWith = PositiveIntegerListField.class,
        converter = PositiveIntegerListField.class,
        variableArity = true)
    List<Integer> integers;
  }

  private static class FakeShortParameter {
    @Parameter(
        names = {"--Sfield"},
        validateWith = PositiveShortListField.class,
        converter = PositiveShortListField.class,
        variableArity = true)
    List<Short> shorts;
  }

  @Test
  public void testCheckPositiveList() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeIntegerParameter(), new String[] {"--Ifield", "-1,2,3"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeIntegerParameter(), new String[] {"--Ifield", "0"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeIntegerParameter(), new String[] {"--Ifield", "0,1,-3"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeShortParameter(), new String[] {"--Ifield", "-1,2,3"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeShortParameter(), new String[] {"--Ifield", "0"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeShortParameter(), new String[] {"--Ifield", "0,1,-3"}));
  }

  @Test
  public void testCheckEmptyList() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeIntegerParameter(), new String[] {"--Ifield", "1,,4"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeShortParameter(), new String[] {"--Ifield", "1,,6"}));
  }
}
