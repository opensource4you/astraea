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
package org.astraea.common.csv;

import static org.astraea.it.Utils.createTempDirectory;
import static org.astraea.it.Utils.mkdir;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import org.astraea.common.Utils;
import org.junit.jupiter.api.Test;

public class CsvWriterTest {
  private final String DATA_MAME = "20220202_AAA888_min.dat";

  @Test
  void differentLineLengthsErrorTest() {
    var local_csv = createTempDirectory("local_CSV");
    var sink = mkdir(local_csv + "/sink");
    var target = new File(sink + "/" + DATA_MAME);

    try (var writer =
        CsvWriter.builder(Utils.packException(() -> new FileWriter(target))).build()) {
      writer.append(
          List.of(
              "TIMESTAMP,RECORD,StartTime2,Batt_V_Min,Rain_mm_Tot,SlrFD_kW_Avg,SlrTF_MJ_Tot,WS_ms_WVc(1),WS_ms_WVc(2),WS_ms_S_WVT,MaxWS_ms_Max,MaxWS_ms_TMx,AirT_C_Avg,AirT_C_Max,AirT_C_TMx,AirT_C_Min,AirT_C_TMn,VP_hPa_Avg,BP_hPa_Max,BP_hPa_TMx,BP_hPa_Min,BP_hPa_TMn,RH_Max,RH_Min,RHT_C_Max,RHT_C_Min,TiltNS_deg_Max,TiltNS_deg_TMx,TiltNS_deg_Min,TiltNS_deg_TMn,TiltWE_deg_Max,TiltWE_deg_TMx,TiltWE_deg_Min,CVMeta"
                  .split(",")));
      writer.append(
          List.of(
              "2015-12-05 00:00:00,333,2015-12-07 23:59:00,10.53,0,0.112,9.638579,20.8,16.42,0,29.52,2019-12-07 02:36:00,16.69,14.5,2019-12-07 19:01:00,15.6,2019-12-07 04:59:00,14.91368,1024.1,2019-12-07 22:44:00,1019.6,2019-12-07 01:22:00,90,65.3,18.2,15.7,26,2019-12-07 01:16:00,-33.4,2019-12-07 01:26:00,61.1,2019-12-07 05:24:00,-48,013CAMPBELLCLIM50501VUE-500001112"
                  .split(",")));
      assertThrows(
          RuntimeException.class,
          () ->
              writer.append(
                  List.of(
                      "2015-12-05 00:00:00,333,2015-12-07 23:59:00,10.53,0,0.112,9.638579,20.8,16.42,0,29.52,2019-12-07 02:36:00,16.69,14.5,2019-12-07 19:01:00,15.6,2019-12-07 04:59:00,14.91368,1024.1,2019-12-07 22:44:00,1019.6,2019-12-07 01:22:00,90,65.3,18.2,15.7,26,2019-12-07 01:16:00,-33.4,2019-12-07 01:26:00,61.1,2019-12-07 05:24:00"
                          .split(","))));
    }
  }

  @Test
  void nullErrorTest() {
    var local_csv = createTempDirectory("local_CSV");
    var sink = mkdir(local_csv + "/sink");
    var target = new File(sink + "/" + DATA_MAME);
    try (var writer =
        CsvWriter.builder(Utils.packException(() -> new FileWriter(target))).build()) {
      assertThrows(RuntimeException.class, () -> writer.append(null));
    }
  }
}
