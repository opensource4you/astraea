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
package org.astraea.app.CleanCsv;

import com.beust.jcommander.ParameterException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.csv.CsvReaderBuilder;
import org.astraea.common.csv.CsvWriterBuilder;

public class CleanCsv {
  public static List<Path> getListOfFiles(String dir, String target) {
    try {
      return Files.find(
              Paths.get(dir),
              999,
              (path, ar) -> {
                var file = path.toFile();
                return !file.isDirectory() && file.getName().contains(target);
              })
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Process all .dat files in the folder
   *
   * @param args args[0]:source path, args[1]:sink path
   */
  public static void main(String[] args) {
    final var SOURCE_DIRECTORY = args[0];
    final var SINK_DIRECTORY = args[1];
    if (Objects.equals(SOURCE_DIRECTORY, SINK_DIRECTORY)) {
      throw new ParameterException("SOURCE_DIRECTORY should not equal to SINK_DIRECTORY.");
    }
    var listOfFiles = getListOfFiles(SOURCE_DIRECTORY, ".dat");

    listOfFiles.forEach(
        path -> {
          var pathSplit = path.toString().split("/");
          var csvName = Arrays.stream(pathSplit).skip(pathSplit.length - 1).findFirst().orElse("/");

          try (var reader =
                  CsvReaderBuilder.of(Utils.packException(() -> new FileReader(path.toFile())))
                      .build();
              var writer =
                  CsvWriterBuilder.of(
                          Utils.packException(() -> new FileWriter(SINK_DIRECTORY + "/" + csvName)))
                      .build()) {
            var headers =
                Arrays.stream(
                        new String[] {
                          reader.rawNext().get(1)
                              + "_"
                              + Arrays.stream(csvName.split("\\.")).findFirst().orElse("")
                        })
                    .collect(Collectors.toList());

            writer.rawAppend(headers);
            writer.append(reader.next());
            reader.skip(2);
            while (reader.hasNext()) {
              writer.append(reader.next());
            }
            writer.flush();
          }
        });
  }
}
