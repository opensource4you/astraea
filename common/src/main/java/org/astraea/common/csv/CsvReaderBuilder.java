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

import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.IntStream;
import org.astraea.common.Utils;

public class CsvReaderBuilder {
  private final Reader reader;
  private Boolean blankLine = false;

  CsvReaderBuilder(Reader source) {
    this.reader = source;
  }

  public CsvReaderBuilder blankLine(boolean allow) {
    this.blankLine = allow;
    return this;
  }

  public CsvReader build() {
    return new CsvReaderImpl(reader, blankLine);
  }

  private static class CsvReaderImpl implements CsvReader {
    private static final String SEPARATOR = ",";
    private final BufferedReader reader;
    private final boolean blankLine;
    private long currentLine = 0;
    private int genericLength = -1;
    private String[] nextLine;

    private CsvReaderImpl(Reader reader, boolean blankLine) {
      this.reader = new BufferedReader(requireNonNull(reader, "reader is null"));
      this.blankLine = blankLine;
    }

    @Override
    public boolean hasNext() {
      if (nextLine == null) {
        nextLine =
            Utils.packException(
                () ->
                    Optional.ofNullable(reader.readLine())
                        .map(line -> line.split(SEPARATOR))
                        .orElse(null));
        if (nextLine != null) currentLine++;
      }
      return nextLine != null;
    }

    /**
     * Reads the next line from the buffer and converts to a List<String>.Checking that each row is
     * of equal length is used to ensure the consistency of the csv data. If there are no more
     * inputs, throw error. Note: It should only be used in the csv body.
     *
     * @return A List<String> with each comma-separated element as a separate entry .
     */
    @Override
    public List<String> next() {
      List<String> strings = rawNext();
      if (genericLength == -1) genericLength = strings.size();
      else if (genericLength != strings.size()) {
        if (blankLine && String.join("", strings).isBlank()) {
          try {
            rawNext();
          } catch (NoSuchElementException e) {
            return strings;
          }
        }
        throw new RuntimeException(
            "The "
                + currentLine
                + " line does not meet the criteria. Each row of data should be equal in length.");
      }

      return strings;
    }

    @Override
    public List<String> rawNext() {
      if (!hasNext()) {
        throw new NoSuchElementException("There is no next line.");
      }
      try {
        return Arrays.stream(nextLine).toList();
      } finally {
        nextLine = null;
      }
    }

    @Override
    public void skip(int num) {
      if (num > 0) {
        currentLine = currentLine + num;
        IntStream.range(0, num).forEach((ignored) -> Utils.packException(reader::readLine));
        nextLine = null;
      }
    }

    @Override
    public void close() {
      Utils.close(reader);
    }
  }
}
