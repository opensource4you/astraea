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
import java.util.Map;
import org.astraea.common.DataRate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringDataRateMapFieldTest {
  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        converter = StringDataRateMapField.class)
    Map<String, DataRate> value;
  }

  @Test
  void testConverter() {
    var param =
        Argument.parse(
            new FakeParameter(), new String[] {"--field", "test-0:60MB/s,test-1:87GB/h"});
    Assertions.assertEquals(2, param.value.size());
    Assertions.assertEquals(DataRate.MB.of(60).perSecond(), param.value.get("test-0"));
    Assertions.assertEquals(DataRate.GB.of(87).perHour(), param.value.get("test-1"));

    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeParameter(), new String[] {"--field", "t-0"}));
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeParameter(), new String[] {"--field", "t-0:"}));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Argument.parse(new FakeParameter(), new String[] {"--field", "ill-0:50"}));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Argument.parse(new FakeParameter(), new String[] {"--field", "ill-0:MB/s"}));
  }
}
