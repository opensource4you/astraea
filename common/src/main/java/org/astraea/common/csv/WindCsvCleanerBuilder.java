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

import static org.astraea.common.Utils.fileLineNum;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;
import org.astraea.common.Utils;

public class WindCsvCleanerBuilder implements OwnCsvCleanerBuilder {
  private final CSVReaderBuilder csvReaderBuilder;
  private final Path source;
  private final long lineNum;

  /**
   * WindCsvCleanerBuilder is used to clean the csv data of wind energy.
   *
   * @param source The reader to an underlying CSV source.
   */
  public WindCsvCleanerBuilder(Path source) {
    this.lineNum = fileLineNum(source);
    this.source = source;
    this.csvReaderBuilder =
        new CSVReaderBuilder(Utils.packException(() -> new FileReader(source.toFile())));
  }

  @Override
  public OwnCsvCleaner build() {
    return new WindCsvCleaner(csvReaderBuilder.build(), source, lineNum);
  }

  public static class WindCsvCleaner extends OwnCsvCleaner {
    private final String[] headers;

    protected WindCsvCleaner(CSVReader csvReader, Path source, long lineNum) {
      super(csvReader, source, lineNum);
      var pathSplit = source.toString().split("/");
      var csvName = Arrays.stream(pathSplit).skip(pathSplit.length - 1).findFirst().orElse("/");
      headers =
          new String[] {
            Utils.packException(() -> csvReader.readNext()[1])
                + "_"
                + Arrays.stream(csvName.split("\\.")).findFirst().orElse("")
          };
      currentLine++;
    }

    @Override
    public String[] headers() {
      return headers;
    }
  }
}
