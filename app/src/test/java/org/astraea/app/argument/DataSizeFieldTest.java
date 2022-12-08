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

import org.astraea.common.DataUnit;
import org.junit.jupiter.api.Test;

class DataSizeFieldTest {
  @Test
  void parseDataSize() {
    var converter = new DataSizeField();
    assertEquals(DataUnit.Bit.of(100).toString(), converter.convert("100Bit").toString());
    assertEquals(DataUnit.Kb.of(100).toString(), converter.convert("100 Kb").toString());
    assertEquals(DataUnit.Mb.of(100).toString(), converter.convert("100 Mb").toString());
    assertEquals(DataUnit.Gb.of(100).toString(), converter.convert("100 Gb").toString());
    assertEquals(DataUnit.Tb.of(100).toString(), converter.convert("100 Tb").toString());
    assertEquals(DataUnit.Pb.of(100).toString(), converter.convert("100 Pb").toString());
    assertEquals(DataUnit.Eb.of(100).toString(), converter.convert("100 Eb").toString());
    assertEquals(DataUnit.Zb.of(100).toString(), converter.convert("100 Zb").toString());
    assertEquals(DataUnit.Yb.of(100).toString(), converter.convert("100 Yb").toString());

    assertEquals(DataUnit.Kib.of(100).toString(), converter.convert("100 Kib").toString());
    assertEquals(DataUnit.Mib.of(100).toString(), converter.convert("100 Mib").toString());
    assertEquals(DataUnit.Gib.of(100).toString(), converter.convert("100 Gib").toString());
    assertEquals(DataUnit.Tib.of(100).toString(), converter.convert("100 Tib").toString());
    assertEquals(DataUnit.Pib.of(100).toString(), converter.convert("100 Pib").toString());
    assertEquals(DataUnit.Eib.of(100).toString(), converter.convert("100 Eib").toString());
    assertEquals(DataUnit.Zib.of(100).toString(), converter.convert("100 Zib").toString());
    assertEquals(DataUnit.Yib.of(100).toString(), converter.convert("100 Yib").toString());

    assertEquals(DataUnit.Byte.of(100).toString(), converter.convert("100Byte").toString());
    assertEquals(DataUnit.KB.of(100).toString(), converter.convert("100 KB").toString());
    assertEquals(DataUnit.MB.of(100).toString(), converter.convert("100 MB").toString());
    assertEquals(DataUnit.GB.of(100).toString(), converter.convert("100 GB").toString());
    assertEquals(DataUnit.TB.of(100).toString(), converter.convert("100 TB").toString());
    assertEquals(DataUnit.PB.of(100).toString(), converter.convert("100 PB").toString());
    assertEquals(DataUnit.EB.of(100).toString(), converter.convert("100 EB").toString());
    assertEquals(DataUnit.ZB.of(100).toString(), converter.convert("100 ZB").toString());
    assertEquals(DataUnit.YB.of(100).toString(), converter.convert("100 YB").toString());

    assertEquals(DataUnit.KiB.of(100).toString(), converter.convert("100 KiB").toString());
    assertEquals(DataUnit.MiB.of(100).toString(), converter.convert("100 MiB").toString());
    assertEquals(DataUnit.GiB.of(100).toString(), converter.convert("100 GiB").toString());
    assertEquals(DataUnit.TiB.of(100).toString(), converter.convert("100 TiB").toString());
    assertEquals(DataUnit.PiB.of(100).toString(), converter.convert("100 PiB").toString());
    assertEquals(DataUnit.EiB.of(100).toString(), converter.convert("100 EiB").toString());
    assertEquals(DataUnit.ZiB.of(100).toString(), converter.convert("100 ZiB").toString());
    assertEquals(DataUnit.YiB.of(100).toString(), converter.convert("100 YiB").toString());

    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB xxx"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB per second"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("xxx 5000 MB"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB 400GB"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("6.00 MB"));
  }
}
