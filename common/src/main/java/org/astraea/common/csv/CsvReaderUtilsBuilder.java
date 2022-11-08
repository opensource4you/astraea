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
import com.opencsv.exceptions.CsvValidationException;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CsvReaderUtilsBuilder {
  private final CSVReaderBuilder csvReaderBuilder;

  /**
   * Construct CSVReaderBuilder so that we can use its build pattern.
   *
   * @param source The reader to an underlying CSV source.
   */
  public CsvReaderUtilsBuilder(File source) {
    try {
      this.csvReaderBuilder = new CSVReaderBuilder(new FileReader(source));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public CsvReaderUtils build() {
    return new CsvReaderUtils(csvReaderBuilder.build());
  }

  public static class CsvReaderUtils implements Closeable {
    private final CSVReader csvReader;

    private CsvReaderUtils(CSVReader csvReader) {
      this.csvReader = csvReader;
    }

    /**
     * Reads the next line from the buffer and converts to a string array.
     *
     * @return A string array with each comma-separated element as a separate entry, or null if
     *     there is no more input.
     */
    public String[] readNext() {
      try {
        return csvReader.readNext();
      } catch (IOException | CsvValidationException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Skip a given number of lines.
     *
     * @param num The number of lines to skip
     */
    public void skip(int num) {
      try {
        csvReader.skip(num);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        csvReader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
