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
package org.astraea.app.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;

public enum ReportFormat implements EnumInfo {
  CSV("csv"),
  JSON("json");

  public static ReportFormat ofAlias(String alias) {
    return EnumInfo.ignoreCaseEnum(ReportFormat.class, alias);
  }

  private final String name;

  ReportFormat(String name) {
    this.name = name;
  }

  @Override
  public String alias() {
    return name;
  }

  public static class ReportFormatConverter implements IStringConverter<ReportFormat> {
    @Override
    public ReportFormat convert(String value) {
      try {
        return ofAlias(value);
      } catch (IllegalArgumentException e) {
        throw new ParameterException(e);
      }
    }
  }

  public static Runnable createFileWriter(
      ReportFormat reportFormat,
      Path path,
      Supplier<Boolean> consumerDone,
      Supplier<Boolean> producerDone,
      Supplier<List<Report>> consumerReporter)
      throws IOException {
    var filePath =
        FileSystems.getDefault()
            .getPath(
                path.toString(),
                "Performance"
                    + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                    + "."
                    + reportFormat);
    var writer = new BufferedWriter(new FileWriter(filePath.toFile()));
    var elements = latencyAndIO(consumerReporter);
    switch (reportFormat) {
      case CSV:
        initCSVFormat(writer, elements);
        return () -> {
          try {
            while (!(producerDone.get() && consumerDone.get())) {
              logToCSV(writer, elements);
              Utils.sleep(Duration.ofSeconds(1));
            }
          } finally {
            Utils.packException(writer::close);
          }
        };
      case JSON:
        writer.write("{");
        return () -> {
          try {
            while (!(producerDone.get() && consumerDone.get())) {
              logToJSON(writer, elements);
              Utils.sleep(Duration.ofSeconds(1));
            }
          } finally {
            Utils.packException(writer::close);
          }
        };
      default:
        throw new IllegalArgumentException("Invalid format.");
    }
  }

  static void initCSVFormat(BufferedWriter writer, List<CSVContentElement> elements)
      throws IOException {
    elements.forEach(element -> Utils.packException(() -> writer.write(element.title() + ", ")));
    writer.newLine();
  }

  static void logToCSV(BufferedWriter writer, List<CSVContentElement> elements) {
    try {
      elements.forEach(element -> Utils.packException(() -> writer.write(element.value() + ", ")));
      writer.newLine();
    } catch (IOException ignore) {
    }
  }

  /** Write to writer. Output: "(timestamp)": { (many metrics ...) } */
  static void logToJSON(BufferedWriter writer, List<CSVContentElement> elements) {
    try {
      writer.write(LocalTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + "\":{");
      elements.forEach(
          element ->
              Utils.packException(
                  () -> writer.write("\"" + element.title() + "\":" + element.value() + ",")));
      writer.write("}");
      writer.newLine();
    } catch (IOException ignore) {
    }
  }

  // Visible for test
  interface CSVContentElement {
    String title();

    String value();

    static CSVContentElement create(String title, Supplier<String> value) {
      return new CSVContentElement() {
        @Override
        public String title() {
          return title;
        }

        @Override
        public String value() {
          return value.get();
        }
      };
    }
  }

  private static List<CSVContentElement> latencyAndIO(Supplier<List<Report>> consumerReporter) {
    var producerReports = Report.producers();
    var consumerReports = consumerReporter.get();
    var elements = new ArrayList<CSVContentElement>();
    elements.add(
        CSVContentElement.create(
            "Time",
            () ->
                LocalTime.now().getHour()
                    + ":"
                    + LocalTime.now().getMinute()
                    + ":"
                    + LocalTime.now().getSecond()));
    elements.add(
        CSVContentElement.create(
            "Publish Max latency (ms)",
            () ->
                Long.toString(
                    producerReports.stream().mapToLong(Report::maxLatency).max().orElse(0))));
    elements.add(
        CSVContentElement.create(
            "End-to-end max latency (ms)",
            () ->
                Long.toString(
                    consumerReports.stream().mapToLong(Report::maxLatency).max().orElse(0))));
    IntStream.range(0, producerReports.size())
        .forEach(
            i -> {
              elements.add(
                  CSVContentElement.create(
                      "Producer[" + i + "] bytes produced",
                      () -> Long.toString(producerReports.get(i).totalBytes())));
              elements.add(
                  CSVContentElement.create(
                      "Producer[" + i + "] average publish latency (ms)",
                      () -> Double.toString(producerReports.get(i).avgLatency())));
            });
    IntStream.range(0, consumerReports.size())
        .forEach(
            i -> {
              elements.add(
                  CSVContentElement.create(
                      "Consumer[" + i + "] bytes produced",
                      () -> Long.toString(consumerReports.get(i).totalBytes())));
              elements.add(
                  CSVContentElement.create(
                      "Consumer[" + i + "] average publish latency (ms)",
                      () -> Double.toString(consumerReports.get(i).avgLatency())));
            });
    return elements;
  }

  @Override
  public String toString() {
    return alias();
  }
}
