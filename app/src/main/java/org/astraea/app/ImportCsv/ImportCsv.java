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
package org.astraea.app.ImportCsv;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.astraea.common.argument.NonEmptyStringField;
import org.astraea.common.csv.CsvReader;
import org.astraea.common.csv.CsvWriterBuilder;

public class ImportCsv {
  /**
   * Process all .dat files in the folder
   *
   * @param args args[0]:source path, args[1]:sink path
   */
  public static void main(String[] args) {
    var count = new AtomicInteger();
    System.out.println("Initialization arguments...");
    var argument = org.astraea.common.argument.Argument.parse(new Argument(), args);

    var source = argument.source.fileSystem();
    var sink = argument.sink.fileSystem();
    var archive = argument.archive.fileSystem();

    System.out.println("Checking source and sink.");
    if (Objects.equals(argument.source.toString(), argument.sink.toString()))
      throw new ParameterException("SOURCE_DIRECTORY should not equal to SINK_DIRECTORY.");

    System.out.println("Starting process.");

    // Process each file in target path.
    source
        .listFiles("/")
        .forEach(
            path -> {
              System.out.println("File " + count + " is being processed.");
              System.out.println("Start processing " + path + ".");

              var csvName = findFinal(path);

              try (var reader = CsvReader.of(new InputStreamReader(source.read(csvName))).build();
                  var writer =
                      CsvWriterBuilder.of(new OutputStreamWriter(sink.write('/' + csvName)))
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

                switch (argument.cleanSource) {
                  case "off":
                    break;
                  case "delete":
                    source.delete(csvName);
                    System.out.println("File: " + path + " has been deleted.");
                    break;
                  case "archive":
                    {
                      if (Objects.equals(
                          argument.source.toString(), argument.archive.fileSystem().toString()))
                        throw new ParameterException(
                            "SourceArchiveDir should not equal to source.");
                      try (var archiveReader =
                              CsvReader.of(new InputStreamReader(source.read(csvName))).build();
                          var archiveWriter =
                              CsvWriterBuilder.of(
                                      new OutputStreamWriter(archive.write('/' + csvName)))
                                  .build()) {
                        while (archiveReader.hasNext()) {
                          archiveWriter.rawAppend(archiveReader.rawNext());
                        }
                      }
                      System.out.println("File: " + path + " has been archived.");
                      source.delete(csvName);
                      System.out.println("File: " + path + " has been deleted.");
                    }
                }
              }
              count.getAndIncrement();
            });

    System.out.println("-------------------------------------------------------");
    System.out.println(" End of program, total of " + count + " files processed.");
    System.out.println("-------------------------------------------------------");
  }

  public static class Argument {
    @Parameter(
        names = {"--source"},
        description = "String: The directory where the uncleaned data is stored.",
        validateWith = FileFrom.Field.class,
        converter = FileFrom.Field.class,
        required = true)
    FileFrom source = FileFrom.of("local:///");

    @Parameter(
        names = {"--sink"},
        description = "String: The directory where the cleaned data is stored.",
        validateWith = FileFrom.Field.class,
        converter = FileFrom.Field.class,
        required = true)
    FileFrom sink = FileFrom.of("local:///");

    @Parameter(
        names = {"--cleanSource"},
        description = "Option to clean up completed files after processing.",
        validateWith = NonEmptyStringField.class)
    String cleanSource = "off";

    @Parameter(
        names = {"--sourceArchiveDir"},
        description = "Source archive directory.",
        validateWith = FileFrom.Field.class,
        converter = FileFrom.Field.class)
    FileFrom archive = FileFrom.of("local:///");
  }

  private static String findFinal(String path) {
    return Arrays.stream(path.split("/")).reduce((first, second) -> second).orElse("");
  }
}
