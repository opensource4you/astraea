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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.format.DateTimeParseException;
import org.astraea.common.DataRate;
import org.junit.jupiter.api.Test;

public class DataRateFieldTest {

  @Test
  void parseDataRateWithException() {
    var converter = new DataRateField();

    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB/ second"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB per second"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("xxx 5000 MB"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("6.00 MB"));

    // unsupported week
    assertThrows(DateTimeParseException.class, () -> converter.convert("5000 MB/Week"));
  }

  @Test
  void parseDataRate() {
    var converter = new DataRateField();

    // test Duration
    assertEquals(DataRate.Bit.of(100), converter.convert("100Bit/S"));
    assertEquals(DataRate.Bit.of(100), converter.convert("100Bit/Second"));
    assertEquals(DataRate.Bit.of(100), converter.convert("100Bit/s"));
    assertEquals(DataRate.Bit.of(100), converter.convert("100Bit/second"));
    assertEquals(DataRate.Bit.of(100), converter.convert("100Bit/seconds"));
    assertEquals(DataRate.GB.of(1), converter.convert("1 GB"));

    // test Size and Unit
    assertEquals(DataRate.Byte.of(100), converter.convert("100Byte"));
    assertEquals(DataRate.KB.of(150), converter.convert("150 KB"));
    assertEquals(DataRate.MB.of(200), converter.convert("200 MB"));
    assertEquals(DataRate.GB.of(250), converter.convert("250 GB"));
    assertEquals(DataRate.TB.of(300), converter.convert("300 TB"));
    assertEquals(DataRate.PB.of(350), converter.convert("350 PB"));
    assertEquals(DataRate.EB.of(400), converter.convert("400 EB"));
    assertEquals(DataRate.ZB.of(450), converter.convert("450 ZB"));
    assertEquals(DataRate.YB.of(500), converter.convert("500 YB"));

    // some example
    assertEquals(DataRate.GB.of(1), converter.convert("1GB"));
  }
}
