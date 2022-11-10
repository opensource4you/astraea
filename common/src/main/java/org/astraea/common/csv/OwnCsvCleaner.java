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
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.astraea.common.Utils;

public abstract class OwnCsvCleaner implements Closeable, Iterator<List<String>> {
  protected final CSVReader csvReader;
  protected final Path path;
  protected long currentLine = 0;
  protected int genericLength = -1;
  protected String[] nextLine;

  protected OwnCsvCleaner(CSVReader csvReader, Path path) {
    this.csvReader = csvReader;
    this.path = path;
    this.nextLine = Utils.packException(csvReader::readNext);
  }

  /**
   * Skip a given number of lines.
   *
   * @param num The number of lines to skip
   */
  public void skip(int num) {
    try {
      currentLine = currentLine + num;
      Utils.requirePositive(num);
      if (num != 1) {
        csvReader.skip(num - 1);
      }
      readNext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads the next line from the buffer and converts to a string array. Note: It should only be
   * used in the csv body.
   *
   * @return A string array with each comma-separated element as a separate entry, or null if there
   *     is no more input.
   */
  @Override
  public List<String> next() {
    var next = readNext();
    currentLine++;
    if (genericLength == -1) genericLength = next.length;
    else if (genericLength != next.length)
      throw new RuntimeException(
          "The "
              + currentLine
              + " line does not meet the criteria. Each row of data should be equal in length.");
    return List.of(next);
  }

  /**
   * Return next line then update.
   *
   * @return next line
   */
  protected String[] readNext() {
    try {
      return nextLine;
    } finally {
      nextLine = Utils.packException(csvReader::readNext);
    }
  }

  /**
   * @return csv headers.
   */
  public abstract List<String> headers();

  @Override
  public boolean hasNext() {
    return nextLine != null;
  }

  @Override
  public void close() {
    Utils.packException(csvReader::close);
  }
}
