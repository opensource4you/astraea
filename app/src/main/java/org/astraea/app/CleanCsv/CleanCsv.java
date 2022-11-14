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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.argument.NonEmptyStringField;
import org.astraea.common.csv.CsvReaderBuilder;
import org.astraea.common.csv.CsvWriterBuilder;

public class CleanCsv {
  public static Stream<Path> getListOfFiles(String dir, String suffixes) {
    return Utils.packException(
        () ->
            Files.find(
                Paths.get(dir),
                999,
                (path, ar) -> {
                  var file = path.toFile();
                  return !file.isDirectory() && file.getName().contains(suffixes);
                }));
  }

  /**
   * Process all .dat files in the folder
   *
   * @param args args[0]:source path, args[1]:sink path
   */
  public static void main(String[] args) {
    var count = new AtomicInteger();
    System.out.println("Initialization arguments...");
    var argument = org.astraea.common.argument.Argument.parse(new Argument(), args);

    System.out.println("Checking source and sink.");
    if (Objects.equals(argument.source, argument.sink)) {
      throw new ParameterException("SOURCE_DIRECTORY should not equal to SINK_DIRECTORY.");
    }
    System.out.println("Starting process.");
    try (var listOfFiles = getListOfFiles(argument.source, argument.suffixes)) {
      listOfFiles.forEach(
          path -> {
            System.out.println("File " + count + " is being processed.");
            System.out.println("Start processing " + path + ".");
            var pathSplit = path.toString().split("/");
            var csvName =
                Arrays.stream(pathSplit).skip(pathSplit.length - 1).findFirst().orElse("/");

            try (var reader =
                    CsvReaderBuilder.of(Utils.packException(() -> new FileReader(path.toFile())))
                        .build();
                var writer =
                    CsvWriterBuilder.of(
                            Utils.packException(
                                () -> new FileWriter(argument.sink + "/" + csvName)))
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
              System.out.println("File: " + path + " has been processed.");
              if (argument.deletion) {
                Utils.packException(() -> Files.delete(path));
                System.out.println("File: " + path + " has been deleted.");
              }
            }
            count.getAndIncrement();
          });
    }
    System.out.println("-------------------------------------------------------");
    System.out.println(" End of program, total of " + count + " files processed.");
    System.out.println("-------------------------------------------------------");
  }

  public static class Argument {
    @Parameter(
        names = {"--source"},
        description = "String: The directory where the uncleaned data is stored.",
        validateWith = NonEmptyStringField.class,
        required = true)
    String source = null;

    @Parameter(
        names = {"--sink"},
        description = "String: The directory where the cleaned data is stored.",
        validateWith = NonEmptyStringField.class,
        required = true)
    String sink = null;

    @Parameter(
        names = {"--suffixes"},
        description = "String: Cleaned file suffixes.",
        validateWith = NonEmptyStringField.class,
        required = true)
    String suffixes = null;

    @Parameter(
        names = {"--deletion"},
        description = "Boolean: Determine if the cleaned data needs to be deleted. Default:false",
        arity = 1)
    Boolean deletion = false;
  }
}
