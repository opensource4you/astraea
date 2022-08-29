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
package org.astraea.app.argument;

import com.beust.jcommander.Parameter;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PositiveShortListFieldTest {
  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        listConverter = PositiveShortListField.class,
        validateWith = PositiveShortListField.class,
        variableArity = true)
    List<Short> value;
  }

  @Test
  public void testShortListConvert() {
    var param = Argument.parse(new FakeParameter(), new String[] {"--field", "1,2,3"});
    var param1 = Argument.parse(new FakeParameter(), new String[] {"--field", "5,10,2"});

    Assertions.assertEquals(List.of((short) 1, (short) 2, (short) 3), param.value);
    Assertions.assertEquals(List.of((short) 5, (short) 10, (short) 2), param1.value);
  }
}
