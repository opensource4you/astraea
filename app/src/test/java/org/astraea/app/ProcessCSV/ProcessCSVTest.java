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
package org.astraea.app.ProcessCSV;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.astraea.app.processCSV.ProcessCSV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProcessCSVTest {
  private final String DATA_MAME = "20220202_AAA888_min.dat";

  @Test
  void run() throws IOException {
    Path local_csv = createTempDirectory("local_CSV");
    Path source = mkdir(local_csv.toString() + "/source");
    Path sink = mkdir(local_csv.toString() + "/sink");
    writeCSV(source);
    ProcessCSV.main(new String[] {source.toString(), sink.toString()});

    var target = new File(sink + "/" + DATA_MAME);
    assertTrue(Files.exists(target.toPath()));
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(target.toString())).build()) {
      assertEquals(
          Arrays.stream(reader.readNext()).findFirst().orElse(""),
          "CR1000(2017)_20220202_AAA888_min");
      assertEquals(
          mkString(Arrays.stream(reader.readNext()).collect(Collectors.toList())),
          "\"TIMESTAMP\",\"RECORD\",\"StartTime2\",\"Batt_V_Min\",\"Rain_mm_Tot\",\"SlrFD_kW_Avg\",\"SlrTF_MJ_Tot\",\"WS_ms_WVc(1)\",\"WS_ms_WVc(2)\",\"WS_ms_S_WVT\",\"MaxWS_ms_Max\",\"MaxWS_ms_TMx\",\"AirT_C_Avg\",\"AirT_C_Max\",\"AirT_C_TMx\",\"AirT_C_Min\",\"AirT_C_TMn\",\"VP_hPa_Avg\",\"BP_hPa_Max\",\"BP_hPa_TMx\",\"BP_hPa_Min\",\"BP_hPa_TMn\",\"RH_Max\",\"RH_Min\",\"RHT_C_Max\",\"RHT_C_Min\",\"TiltNS_deg_Max\",\"TiltNS_deg_TMx\",\"TiltNS_deg_Min\",\"TiltNS_deg_TMn\",\"TiltWE_deg_Max\",\"TiltWE_deg_TMx\",\"TiltWE_deg_Min\",\"CVMeta\"");
      assertEquals(
          mkString(Arrays.stream(reader.readNext()).collect(Collectors.toList())),
          "\"2015-12-05 00:00:00\",\"333\",\"2015-12-07 23:59:00\",\"10.53\",\"0\",\"0.112\",\"9.638579\",\"20.8\",\"16.42\",\"0\",\"29.52\",\"2019-12-07 02:36:00\",\"16.69\",\"14.5\",\"2019-12-07 19:01:00\",\"15.6\",\"2019-12-07 04:59:00\",\"14.91368\",\"1024.1\",\"2019-12-07 22:44:00\",\"1019.6\",\"2019-12-07 01:22:00\",\"90\",\"65.3\",\"18.2\",\"15.7\",\"26\",\"2019-12-07 01:16:00\",\"-33.4\",\"2019-12-07 01:26:00\",\"61.1\",\"2019-12-07 05:24:00\",\"-48\",\"013CAMPBELLCLIM50501VUE-500001112\"");
    } catch (CsvValidationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void getListOfFilesTest() throws IOException {
    var str = "test_dat" + new Random().nextInt();
    var tempFile = File.createTempFile(str, ".dat");
    Assertions.assertEquals(
        ProcessCSV.getListOfFiles(tempFile.getAbsolutePath(), str).get(0), tempFile.toPath());
  }

  void writeCSV(Path source) throws IOException {
    try (CSVWriter writer =
        new CSVWriter(new FileWriter(source.toString() + "/" + DATA_MAME, true))) {

      String[] record1 =
          "TOA5,CR1000(2017),CR1000,82129,CR1000.Std.31,CPU:CR10002017_ENV_20190201_Slowsequence_timestamp_corr.CR1,56749,Climavue50_daily"
              .split(",");
      String[] record2 =
          "TIMESTAMP,RECORD,StartTime2,Batt_V_Min,Rain_mm_Tot,SlrFD_kW_Avg,SlrTF_MJ_Tot,WS_ms_WVc(1),WS_ms_WVc(2),WS_ms_S_WVT,MaxWS_ms_Max,MaxWS_ms_TMx,AirT_C_Avg,AirT_C_Max,AirT_C_TMx,AirT_C_Min,AirT_C_TMn,VP_hPa_Avg,BP_hPa_Max,BP_hPa_TMx,BP_hPa_Min,BP_hPa_TMn,RH_Max,RH_Min,RHT_C_Max,RHT_C_Min,TiltNS_deg_Max,TiltNS_deg_TMx,TiltNS_deg_Min,TiltNS_deg_TMn,TiltWE_deg_Max,TiltWE_deg_TMx,TiltWE_deg_Min,CVMeta"
              .split(",");
      String[] record3 =
          "TS,RN,,Volts,mm,kW/m^2,MJ/m^2,meters/second,meters/second,,meters/second,meters/second,Deg C,Deg C,Deg C,Deg C,Deg C,hPa,hPa,hPa,hPa,hPa,%,%,Deg C,Deg C,degrees,degrees,degrees,degrees,degrees,degrees,degrees,"
              .split(",");
      String[] record4 =
          ",,Smp,Min,Tot,Avg,Tot,WVc,WVc,Tot,Max,TMx,Avg,Max,TMx,Min,TMn,Avg,Max,TMx,Min,TMn,Max,Min,Max,Min,Max,TMx,Min,TMn,Max,TMx,Min,Smp"
              .split(",");
      String[] record5 =
          "2015-12-05 00:00:00,333,2015-12-07 23:59:00,10.53,0,0.112,9.638579,20.8,16.42,0,29.52,2019-12-07 02:36:00,16.69,14.5,2019-12-07 19:01:00,15.6,2019-12-07 04:59:00,14.91368,1024.1,2019-12-07 22:44:00,1019.6,2019-12-07 01:22:00,90,65.3,18.2,15.7,26,2019-12-07 01:16:00,-33.4,2019-12-07 01:26:00,61.1,2019-12-07 05:24:00,-48,013CAMPBELLCLIM50501VUE-500001112"
              .split(",");

      writer.writeNext(record1);
      writer.writeNext(record2);
      writer.writeNext(record3);
      writer.writeNext(record4);
      writer.writeNext(record5);
    }
  }

  private Path createTempDirectory(String str) {
    try {
      return Files.createTempDirectory(str);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path mkdir(String string) {
    var file = new File(string);
    file.mkdir();
    return file.toPath();
  }

  private String mkString(List<String> arr) {
    return "\"" + String.join("\",\"", arr) + "\"";
  }
}
