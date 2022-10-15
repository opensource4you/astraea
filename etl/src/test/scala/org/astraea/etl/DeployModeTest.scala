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
package org.astraea.etl

import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class DeployModeTest {
  @Test def deployModePatternTest(): Unit = {
    assertTrue(!DeployMode.deployMatch(""))
    assertTrue(!DeployMode.deployMatch("123"))
    assertTrue(DeployMode.deployMatch("local[2]"))

    assertTrue(!DeployMode.deployMatch(""))
    assertTrue(!DeployMode.deployMatch("abc"))
    assertTrue(!DeployMode.deployMatch("spark://0.0.0.0"))
  }

  @Test def ofTest(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeployMode.of("123xxx")
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeployMode.of("local")
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeployMode.of("spar://0.0.0.0")
    )
  }
}
