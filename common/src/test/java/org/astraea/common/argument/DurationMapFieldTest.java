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

import com.beust.jcommander.ParameterException;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DurationMapFieldTest {
  @Test
  void testDurationInMap() {
    var field = new DurationMapField();
    var result = field.convert("kill:3s,unsubscribe:9s");
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(Duration.ofSeconds(3), result.get("kill"));
    Assertions.assertEquals(Duration.ofSeconds(9), result.get("unsubscribe"));
  }

  @Test
  void testNonMap() {
    var field = new DurationMapField();
    Assertions.assertThrows(ParameterException.class, () -> field.validate("a", "bb"));
    Assertions.assertThrows(ParameterException.class, () -> field.validate("a", "bb="));
  }

  @Test
  void testIllegal() {
    var field = new DurationMapField();
    Assertions.assertThrows(IllegalArgumentException.class, () -> field.convert("kill:3S"));
    Assertions.assertThrows(ParameterException.class, () -> field.convert("kill=3s"));
    Assertions.assertThrows(ParameterException.class, () -> field.convert("kill,3S"));
  }
}
