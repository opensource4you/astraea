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

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
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
    assertEquals(DataRate.Bit.of(100).perSecond(), converter.convert("100Bit/S"));
    assertEquals(DataRate.Bit.of(100).perSecond(), converter.convert("100Bit/Second"));
    assertEquals(DataRate.Bit.of(100).perSecond(), converter.convert("100Bit/s"));
    assertEquals(DataRate.Bit.of(100).perSecond(), converter.convert("100Bit/second"));
    assertEquals(DataRate.Bit.of(100).perSecond(), converter.convert("100Bit/seconds"));
    assertEquals(DataRate.Bit.of(100).perMinute(), converter.convert("100Bit/m"));
    assertEquals(DataRate.Bit.of(100).perMinute(), converter.convert("100Bit/minute"));
    assertEquals(DataRate.Bit.of(100).perMinute(), converter.convert("100Bit/minutes"));
    assertEquals(DataRate.Bit.of(100).perHour(), converter.convert("100Bit/h"));
    assertEquals(DataRate.Bit.of(100).perHour(), converter.convert("100Bit/hour"));
    assertEquals(DataRate.Bit.of(100).perHour(), converter.convert("100Bit/hours"));
    assertEquals(DataRate.Bit.of(100).perDay(), converter.convert("100Bit/d"));
    assertEquals(DataRate.Bit.of(100).perDay(), converter.convert("100Bit/day"));
    assertEquals(DataRate.Bit.of(100).perDay(), converter.convert("100Bit/days"));
    assertEquals(
        DataRate.Bit.of(100).over(Duration.of(20345, ChronoUnit.MILLIS)),
        converter.convert("100Bit/PT20.345S"));
    assertEquals(
        DataRate.Bit.of(100).over(Duration.of(15, ChronoUnit.MINUTES)),
        converter.convert("100Bit/PT15M"));

    // test Space
    assertEquals(DataRate.Bit.of(100).perHour(), converter.convert("100 Bit/hour"));
    assertEquals(DataRate.MB.of(250).perDay(), converter.convert("250 MB/d"));
    assertEquals(DataRate.GB.of(1).perSecond(), converter.convert("1 GB"));

    // test Size and Unit
    assertEquals(DataRate.Byte.of(100).perSecond(), converter.convert("100Byte"));
    assertEquals(DataRate.KB.of(150).perSecond(), converter.convert("150 KB"));
    assertEquals(DataRate.MB.of(200).perSecond(), converter.convert("200 MB"));
    assertEquals(DataRate.GB.of(250).perSecond(), converter.convert("250 GB"));
    assertEquals(DataRate.TB.of(300).perSecond(), converter.convert("300 TB"));
    assertEquals(DataRate.PB.of(350).perSecond(), converter.convert("350 PB"));
    assertEquals(DataRate.EB.of(400).perSecond(), converter.convert("400 EB"));
    assertEquals(DataRate.ZB.of(450).perSecond(), converter.convert("450 ZB"));
    assertEquals(DataRate.YB.of(500).perSecond(), converter.convert("500 YB"));

    // some example
    assertEquals(DataRate.GB.of(1).perSecond(), converter.convert("1GB"));
    assertEquals(DataRate.KiB.of(175).perMinute(), converter.convert("175KiB/M"));
    assertEquals(DataRate.MB.of(200).perDay(), converter.convert("200 MB/d"));
    assertEquals(DataRate.GB.of(17).perHour(), converter.convert("17GB/Hour"));
    assertEquals(
        DataRate.KB.of(111).over(Duration.of(25, ChronoUnit.SECONDS)),
        converter.convert("111 KB/PT25S"));
    assertEquals(
        DataRate.KB.of(1024).over(Duration.ofDays(2).plusHours(3)),
        converter.convert("1024KB/P2DT3H"));
  }
}
