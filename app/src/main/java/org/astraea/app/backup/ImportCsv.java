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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.astraea.common.argument.NonEmptyStringField;
import org.astraea.common.argument.URIField;
import org.astraea.common.csv.CsvReaderBuilder;
import org.astraea.common.csv.CsvWriterBuilder;
import org.astraea.fs.FileSystem;

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

    try (var source = FileSystem.of(argument.source);
        var sink = FileSystem.of(argument.sink);
        var archive = FileSystem.of(argument.source)) {

      System.out.println("Checking source and sink.");
      nonEqualPath(argument.source, argument.sink);

      System.out.println("Starting process.");

      // Process each file in target path.
      source
          .listFiles(argument.source.getPath())
          .forEach(
              sourcePath -> {
                System.out.println("File " + count + " is being processed.");
                System.out.println("Start processing " + sourcePath + ".");

                var csvName = findFinal(sourcePath);
                var sinkPath = argument.sink.getPath() + "/" + csvName;
                var archivePath = argument.archive.getPath() + "/" + csvName;

                try (var reader =
                        CsvReaderBuilder.of(new InputStreamReader(source.read(sourcePath)))
                            .build();
                    var writer =
                        CsvWriterBuilder.builder(new OutputStreamWriter(sink.write(sinkPath)))
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
                  System.out.println("File: " + sourcePath + " has been processed.");

                  switch (argument.cleanSource) {
                    case "off":
                      break;
                    case "delete":
                      source.delete(sourcePath);
                      System.out.println("File: " + sourcePath + " has been deleted.");
                      break;
                    case "archive":
                      {
                        nonEqualPath(argument.source, argument.archive);

                        try (var archiveReader =
                                CsvReaderBuilder.of(new InputStreamReader(source.read(sourcePath)))
                                    .build();
                            var archiveWriter =
                                CsvWriterBuilder.builder(
                                        new OutputStreamWriter(archive.write(archivePath)))
                                    .build()) {
                          while (archiveReader.hasNext()) {
                            archiveWriter.rawAppend(archiveReader.rawNext());
                          }
                        }
                        System.out.println("File: " + sourcePath + " has been archived.");
                        source.delete(sourcePath);
                        System.out.println("File: " + sourcePath + " has been deleted.");
                      }
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
        converter = URIField.class,
        required = true)
    URI source = URI.create("local:///");

    @Parameter(
        names = {"--sink"},
        description = "String: The directory where the cleaned data is stored.",
        validateWith = URIField.class,
        required = true)
    URI sink = URI.create("local:///");

    @Parameter(
        names = {"--cleanSource"},
        description = "Option to clean up completed files after processing.",
        validateWith = NonEmptyStringField.class)
    String cleanSource = "off";

    @Parameter(
        names = {"--sourceArchiveDir"},
        description = "Source archive directory.",
        validateWith = URIField.class)
    URI archive = URI.create("local:///");
  }

  static void nonEqualPath(URI uri1, URI uri2) {
    if (uri1.getScheme().equals(uri2.getScheme()))
      if (uri1.getScheme().equals("local") && uri1.getPath().equals(uri2.getPath()))
        throw new ParameterException(uri1 + " should not equal to " + uri2 + ".");
      else if (uri1.getScheme().equals("ftp")
          && uri1.getAuthority().equals(uri2.getAuthority())
          && uri1.getPath().equals(uri2.getPath())) {
        throw new ParameterException(uri1 + " should not equal to " + uri2 + ".");
      }
  }

  private static String findFinal(String path) {
    return Arrays.stream(path.split("/")).reduce((first, second) -> second).orElse("");
  }
}
