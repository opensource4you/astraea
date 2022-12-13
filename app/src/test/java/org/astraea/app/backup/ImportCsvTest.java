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
package org.astraea.app.backup;

import static org.astraea.app.backup.ImportCsv.nonEqualPath;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beust.jcommander.ParameterException;
import com.opencsv.CSVReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.csv.CsvWriter;
import org.astraea.fs.local.LocalFileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ImportCsvTest {
  private final String DATA_MAME = "20220202_AAA888_min.dat";

  @Test
  void runTest() throws IOException {
    try (var localFileSystem = new LocalFileSystem(Configuration.of(Map.of())); ) {
      var local_csv = createTempDirectory("local_CSV");
      var source = local_csv.toString() + "/source";
      localFileSystem.mkdir(source);
      var sink = local_csv + "/sink";

      List<String[]> ansLists = new ArrayList<>();
      testCsvGenerator(Path.of(source), ansLists, DATA_MAME, 0);
      String[] arguments = {"--source", "local:" + source, "--sink", "local:" + sink};
      ImportCsv.main(arguments);

      var target = new File(sink + "/" + DATA_MAME);
      assertTrue(Files.exists(target.toPath()));
      checkFile(target, ansLists);
    }
  }

  @Test
  void skipHeadTest() {
    try (var localFileSystem = new LocalFileSystem(Configuration.of(Map.of())); ) {
      var local_csv = Utils.packException(() -> createTempDirectory("local_CSV"));
      var source = local_csv.toString() + "/source";
      localFileSystem.mkdir(source);
      var sink = local_csv + "/sink";

      List<String[]> ansLists = new ArrayList<>();
      testCsvGenerator(Path.of(source), ansLists, DATA_MAME, 2);
      String[] arguments = {
        "--source", "local:" + source, "--sink", "local:" + sink, "--headSkip", "2"
      };
      ImportCsv.main(arguments);

      var target = new File(sink + "/" + DATA_MAME);
      assertTrue(Files.exists(target.toPath()));
      Utils.packException(() -> checkFile(target, ansLists));
    }
  }

  @Test
  void deleteTest() {
    try (var localFileSystem = new LocalFileSystem(Configuration.of(Map.of()))) {
      Path local_csv = Utils.packException(() -> createTempDirectory("local_CSV"));
      var source = local_csv.toString() + "/source";
      localFileSystem.mkdir(source);
      var sink = local_csv + "/sink";
      localFileSystem.mkdir(sink);

      List<String[]> ansLists = new ArrayList<>();
      List<String[]> lists = testCsvGenerator(Path.of(source), ansLists, DATA_MAME, 0);
      writeCSV(new File(source + "/" + DATA_MAME).toPath(), lists, 0);

      assertTrue(Files.exists(new File(source + "/" + DATA_MAME).toPath()));

      String[] arguments = {
        "--source", "local:" + source, "--sink", "local:" + sink, "--cleanSource", "delete",
      };
      ImportCsv.main(arguments);
      assertFalse(Files.exists(new File(source + "/" + DATA_MAME).toPath()));
    }
  }

  @Test
  void archiveTest() throws IOException {
    try (var localFileSystem = new LocalFileSystem(Configuration.of(Map.of()))) {
      Path local_csv = createTempDirectory("local_CSV");
      var source = local_csv + "/source";
      localFileSystem.mkdir(source);
      var sink = local_csv + "/sink";
      localFileSystem.mkdir(sink);
      var archive = local_csv + "/archive";
      localFileSystem.mkdir(archive);

      List<String[]> ansLists = new ArrayList<>();
      List<String[]> lists = testCsvGenerator(Path.of(source), ansLists, DATA_MAME, 0);
      writeCSV(new File(source + "/" + DATA_MAME).toPath(), lists, 0);

      assertTrue(Files.exists(new File(source + "/" + DATA_MAME).toPath()));

      String[] arguments = {
        "--source",
        "local:" + source,
        "--sink",
        "local:" + sink,
        "--cleanSource",
        "archive",
        "--sourceArchiveDir",
        "local:" + archive
      };
      ImportCsv.main(arguments);
      assertFalse(Files.exists(new File(source + "/" + DATA_MAME).toPath()));
      assertTrue(Files.exists(new File(archive + "/" + DATA_MAME).toPath()));
    }
  }

  @Test
  void multipleFileTest() throws IOException {
    try (var localFileSystem = new LocalFileSystem(Configuration.of(Map.of()))) {
      var name2 = "20220202_AAA999_min.dat";

      Path local_csv = createTempDirectory("local_CSV");
      var source = Path.of(local_csv.toString() + "/source");
      localFileSystem.mkdir(source.toString());
      var sink = local_csv + "/sink";
      localFileSystem.mkdir(sink);

      List<String[]> ansLists1 = new ArrayList<>();
      List<String[]> ansLists2 = new ArrayList<>();
      testCsvGenerator(source, ansLists1, DATA_MAME, 0);
      testCsvGenerator(source, ansLists2, name2, 0);

      assertTrue(Files.exists(new File(source + "/" + DATA_MAME).toPath()));
      assertTrue(Files.exists(new File(source + "/" + name2).toPath()));
      String[] arguments = {"--source", "local:" + source, "--sink", "local:" + sink};
      ImportCsv.main(arguments);

      var target1 = new File(sink + "/" + DATA_MAME);
      var target2 = new File(sink + "/" + name2);

      assertTrue(Files.exists(new File(sink + "/" + DATA_MAME).toPath()));
      assertTrue(Files.exists(new File(sink + "/" + name2).toPath()));

      checkFile(target1, ansLists1);
      checkFile(target2, ansLists2);
    }
  }

  @Test
  void nonEqualPathTest() {
    assertThrows(
        ParameterException.class,
        () -> nonEqualPath(URI.create("local:/home/warren"), URI.create("local:/home/warren")));
    assertDoesNotThrow(
        () ->
            nonEqualPath(
                URI.create("ftp://0.0.0.0:8888/home/warren"), URI.create("local:/home/warren")));
    assertThrows(
        ParameterException.class,
        () ->
            nonEqualPath(
                URI.create("ftp://0.0.0.0:8888/home/warren"),
                URI.create("ftp://0.0.0.0:8888/home/warren")));
    assertDoesNotThrow(
        () ->
            nonEqualPath(
                URI.create("ftp://0.0.0.0:8888/home/warren"),
                URI.create("ftp://0.0.0.0:7777/home/warren")));
  }

  private List<String[]> testCsvGenerator(
      Path source, List<String[]> ansLists, String name, int skip) {
    List<String[]> lists =
        new ArrayList<>(
            List.of(
                "TOA5,CR1000(2017),CR1000,82129,CR1000.Std.31,CPU:CR10002017_ENV_20190201_Slowsequence_timestamp_corr.CR1,56749,Climavue50_daily"
                    .split(","),
                "TIMESTAMP,RECORD,StartTime2,Batt_V_Min,Rain_mm_Tot,SlrFD_kW_Avg,SlrTF_MJ_Tot,WS_ms_WVc(1),WS_ms_WVc(2),WS_ms_S_WVT,MaxWS_ms_Max,MaxWS_ms_TMx,AirT_C_Avg,AirT_C_Max,AirT_C_TMx,AirT_C_Min,AirT_C_TMn,VP_hPa_Avg,BP_hPa_Max,BP_hPa_TMx,BP_hPa_Min,BP_hPa_TMn,RH_Max,RH_Min,RHT_C_Max,RHT_C_Min,TiltNS_deg_Max,TiltNS_deg_TMx,TiltNS_deg_Min,TiltNS_deg_TMn,TiltWE_deg_Max,TiltWE_deg_TMx,TiltWE_deg_Min,CVMeta"
                    .split(","),
                "TS,RN,,Volts,mm,kW/m^2,MJ/m^2,meters/second,meters/second,,meters/second,meters/second,Deg C,Deg C,Deg C,Deg C,Deg C,hPa,hPa,hPa,hPa,hPa,%,%,Deg C,Deg C,degrees,degrees,degrees,degrees,degrees,degrees,degrees,"
                    .split(","),
                ",,Smp,Min,Tot,Avg,Tot,WVc,WVc,Tot,Max,TMx,Avg,Max,TMx,Min,TMn,Avg,Max,TMx,Min,TMn,Max,Min,Max,Min,Max,TMx,Min,TMn,Max,TMx,Min,Smp"
                    .split(",")));
    IntStream.range(0, 300)
        .forEach(
            ignore -> {
              String[] strings = fakeDataGenerator();
              lists.add(strings);
              ansLists.add(strings);
            });
    writeCSV(new File(source + "/" + name).toPath(), lists, skip);

    return lists;
  }

  private void checkFile(File target, List<String[]> ansLists) throws IOException {
    var pathSplit = target.toString().split("/");
    var csvName = Arrays.stream(pathSplit).skip(pathSplit.length - 1).findFirst().orElse("/");
    try (var reader = new CSVReader(new FileReader(target))) {
      assertEquals(
          Arrays.stream(Utils.packException(reader::readNext)).findFirst().orElse(""),
          "CR1000(2017)_" + Arrays.stream(csvName.split("\\.")).findFirst().orElse("/"));
      assertEquals(
          mkString(
              Arrays.stream(Utils.packException(reader::readNext)).collect(Collectors.toList())),
          "\"TIMESTAMP\",\"RECORD\",\"StartTime2\",\"Batt_V_Min\",\"Rain_mm_Tot\",\"SlrFD_kW_Avg\",\"SlrTF_MJ_Tot\",\"WS_ms_WVc(1)\",\"WS_ms_WVc(2)\",\"WS_ms_S_WVT\",\"MaxWS_ms_Max\",\"MaxWS_ms_TMx\",\"AirT_C_Avg\",\"AirT_C_Max\",\"AirT_C_TMx\",\"AirT_C_Min\",\"AirT_C_TMn\",\"VP_hPa_Avg\",\"BP_hPa_Max\",\"BP_hPa_TMx\",\"BP_hPa_Min\",\"BP_hPa_TMn\",\"RH_Max\",\"RH_Min\",\"RHT_C_Max\",\"RHT_C_Min\",\"TiltNS_deg_Max\",\"TiltNS_deg_TMx\",\"TiltNS_deg_Min\",\"TiltNS_deg_TMn\",\"TiltWE_deg_Max\",\"TiltWE_deg_TMx\",\"TiltWE_deg_Min\",\"CVMeta\"");
      Iterator<String[]> iterator = ansLists.iterator();
      IntStream.range(0, 300)
          .forEach(
              ignore ->
                  assertTrue(
                      checkEquality(Utils.packException(reader::readNext), iterator.next()),
                      String.valueOf(ignore)));
      Assertions.assertEquals(fileLineNum(target.toPath()), 302);
    }
  }

  private String[] fakeDataGenerator() {
    return Arrays.stream(
            "2015-12-05 00:00:00,333,2015-12-07 23:59:00,10.53,0,0.112,9.638579,20.8,16.42,0,29.52,2019-12-07 02:36:00,16.69,14.5,2019-12-07 19:01:00,15.6,2019-12-07 04:59:00,14.91368,1024.1,2019-12-07 22:44:00,1019.6,2019-12-07 01:22:00,90,65.3,18.2,15.7,26,2019-12-07 01:16:00,-33.4,2019-12-07 01:26:00,61.1,2019-12-07 05:24:00,-48,013CAMPBELLCLIM50501VUE-500001112"
                .split(","))
        .map(col -> String.join("-", col, Utils.randomString()))
        .toArray(String[]::new);
  }

  private void writeCSV(Path sink, List<String[]> lists, int skip) {
    try (var writer =
        Utils.packException(() -> CsvWriter.builder(new FileWriter(sink.toFile())).build())) {
      IntStream.range(0, skip)
          .forEach(
              ignore ->
                  writer.rawAppend(
                      Arrays.stream(fakeDataGenerator()).collect(Collectors.toList())));
      lists.forEach(line -> writer.rawAppend(Arrays.stream(line).collect(Collectors.toList())));
    }
  }

  private boolean checkEquality(String[] s1, String[] s2) {
    if (s1 == s2) return true;
    if (s1 == null || s2 == null) return false;
    int n = s1.length;
    if (n != s2.length) return false;
    int i = 0;
    while (i < n) {
      if (!s1[i].equals(s2[i])) {
        return false;
      }
      i++;
    }
    return true;
  }

  private Path createTempDirectory(String str) throws IOException {
    return Files.createTempDirectory(str);
  }

  private String mkString(List<String> arr) {
    return "\"" + String.join("\",\"", arr) + "\"";
  }

  private long fileLineNum(Path path) {
    try (var file = Utils.packException(() -> Files.lines(Paths.get(path.toUri())))) {
      return file.count();
    }
  }
}
