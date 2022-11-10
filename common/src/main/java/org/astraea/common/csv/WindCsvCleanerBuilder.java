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

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.Utils;

public class WindCsvCleanerBuilder implements OwnCsvCleanerBuilder {
  private final CSVReaderBuilder csvReaderBuilder;
  private final Path source;
  /**
   * WindCsvCleanerBuilder is used to clean the csv data of wind energy.
   *
   * @param source The reader to an underlying CSV source.
   */
  public WindCsvCleanerBuilder(Path source) {
    this.source = source;
    this.csvReaderBuilder =
        new CSVReaderBuilder(Utils.packException(() -> new FileReader(source.toFile())));
  }

  @Override
  public OwnCsvCleaner build() {
    return new WindCsvCleaner(csvReaderBuilder.build(), source);
  }

  public static class WindCsvCleaner extends OwnCsvCleaner {
    private final List<String> headers;

    protected WindCsvCleaner(CSVReader csvReader, Path source) {
      super(csvReader, source);
      var pathSplit = source.toString().split("/");
      var csvName = Arrays.stream(pathSplit).skip(pathSplit.length - 1).findFirst().orElse("/");
      headers =
          Arrays.stream(
                  new String[] {
                    readNext()[1] + "_" + Arrays.stream(csvName.split("\\.")).findFirst().orElse("")
                  })
              .collect(Collectors.toList());
      currentLine++;
    }

    @Override
    public List<String> headers() {
      return headers;
    }
  }
}
