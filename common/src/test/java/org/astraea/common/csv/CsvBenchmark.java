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

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.Throughput)
public class CsvBenchmark {

  @Benchmark
  public void read(BenchmarkData benchmarkData, Blackhole blackhole) {
    benchmarkData.csvReader.forEachRemaining(blackhole::consume);
  }

  @Benchmark
  public void write(BenchmarkData benchmarkData) {
    benchmarkData.data.forEach(line -> benchmarkData.csvWriter.append(line));
  }

  @State(Scope.Thread)
  public static class BenchmarkData {
    private static final int ROW_COUNT = 15000;
    private final List<List<String>> data = new ArrayList<>();
    private CsvReader csvReader;
    private CsvWriter csvWriter;

    @Setup
    public void initialize() {
      IntStream.range(0, ROW_COUNT)
          .forEach((ignored) -> data.add(List.of("a", "b", "c", "d", "e")));
      csvReader =
          CsvReader.builder(Utils.packException(() -> new FileReader(prepareCsv(data)))).build();
      csvWriter = CsvWriter.builder(FileWriter.nullWriter()).build();
    }

    private static File prepareCsv(List<List<String>> csvData) {
      File csvFile =
          Utils.packException(() -> Files.createTempFile("csv_benchmark", ".csv").toFile());
      try (CsvWriter csvWriter =
          CsvWriter.builder(Utils.packException(() -> new FileWriter(csvFile))).build()) {
        csvData.forEach(csvWriter::append);
        csvWriter.flush();
        return csvFile;
      }
    }
  }

  public static void main(String[] args) throws RunnerException {
    BenchmarkData data = new BenchmarkData();
    data.initialize();

    Options options =
        new OptionsBuilder()
            .verbosity(VerboseMode.NORMAL)
            .resultFormat(ResultFormatType.JSON)
            .result(
                format(
                    "%s/%s-result-%s.json",
                    System.getProperty("java.io.tmpdir"),
                    CsvBenchmark.class.getSimpleName(),
                    ISO_DATE_TIME.format(LocalDateTime.now())))
            .build();

    new Runner(options).run();
  }
}
