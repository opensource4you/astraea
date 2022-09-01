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

import com.beust.jcommander.ParameterException;
import org.astraea.app.performance.Performance;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ListFieldTest {
  @Test
  public void testCheckEmptyList() {
    String[] param = new String[] {"--bootstrap.server", "localhost:9092", "--topics"};

    Assertions.assertThrows(
        ParameterException.class,
        () -> Performance.Argument.parse(new Performance.Argument(), param));
  }

  @Test
  public void testCheckSeparator() {
    var param = new String[] {"--bootstrap.server", "localhost:9092", "--topics", ",,"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> Performance.Argument.parse(new Performance.Argument(), param));
    var param1 = new String[] {"--bootstrap.server", "localhost:9092", "--topics", ",test"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> Performance.Argument.parse(new Performance.Argument(), param1));
    var param2 = new String[] {"--bootstrap.server", "localhost:9092", "--topics", "test,,test1"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> Performance.Argument.parse(new Performance.Argument(), param2));
  }
}
