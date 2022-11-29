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
package org.astraea.gui;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;

public interface Query {

  Query ALL =
      new Query() {
        @Override
        public boolean required(String key) {
          return true;
        }

        @Override
        public boolean required(Map<String, Object> item) {
          return true;
        }
      };

  boolean required(String key);

  boolean required(Map<String, Object> item);

  static Query of(final String text) {
    if (text == null || text.isBlank()) return Query.ALL;
    var subStrings = Arrays.stream(text.split("&&|\\|\\|")).collect(Collectors.toList());
    var ops = new ArrayList<String>();
    for (int i = 0; i != subStrings.size(); ++i) {
      var l = text.indexOf(subStrings.get(i)) + subStrings.get(i).length();
      if (i + 1 == subStrings.size()) break;
      var r = text.indexOf(subStrings.get(i + 1), l);
      var opString = text.substring(l, r);
      if (opString.contains("&&")) ops.add("&&");
      if (opString.contains("||")) ops.add("||");
    }
    if (ops.size() + 1 != subStrings.size())
      throw new IllegalArgumentException("invalid query: " + text);

    var queries =
        subStrings.stream()
            .map(String::trim)
            .flatMap(
                string ->
                    Stream.of(
                        forNumber(string).stream(),
                        forEmpty(string).stream(),
                        forString(string).stream()))
            .flatMap(a -> a)
            .collect(Collectors.toList());
    return new Query() {
      @Override
      public boolean required(String key) {
        return queries.stream().anyMatch(q -> q.required(key));
      }

      @Override
      public boolean required(Map<String, Object> item) {
        Boolean match = null;
        for (var index = 0; index != queries.size(); ++index) {
          if (match == null) match = queries.get(index).required(item);
          else {
            switch (ops.get(index - 1)) {
              case "&&":
                match = match && queries.get(index).required(item);
                break;
              case "||":
                match = match || queries.get(index).required(item);
                break;
              default:
                throw new IllegalArgumentException("unsupported op: " + ops.get(index - 1));
            }
          }
        }
        return match == null || match;
      }
    };
  }

  private static Optional<Query> forNumber(String predicateString) {
    if (predicateString == null || predicateString.isBlank()) return Optional.empty();
    BiFunction<Pattern, String, Query> createSmallPredicate =
        (keyPattern, valueString) ->
            new Query() {
              @Override
              public boolean required(String key) {
                return keyPattern.matcher(key).matches();
              }

              @Override
              public boolean required(Map<String, Object> item) {
                return item.entrySet().stream()
                    .anyMatch(
                        e -> {
                          if (e.getValue() instanceof Number || e.getValue() instanceof String) {
                            try {
                              var value = Long.parseLong(e.getValue().toString());
                              return keyPattern.matcher(e.getKey()).matches()
                                  && value < Long.parseLong(valueString);
                            } catch (NumberFormatException ignored) {
                              // swallow
                            }
                          }
                          if (e.getValue() instanceof DataSize) {
                            var size = ((DataSize) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && size.smallerThan(DataSize.of(valueString));
                          }
                          if (e.getValue() instanceof LocalDateTime) {
                            var time = ((LocalDateTime) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && time.compareTo(LocalDateTime.parse(valueString)) < 0;
                          }
                          return false;
                        });
              }
            };

    BiFunction<Pattern, String, Query> createEqualPredicate =
        (keyPattern, valueString) ->
            new Query() {
              @Override
              public boolean required(String key) {
                return keyPattern.matcher(key).matches();
              }

              @Override
              public boolean required(Map<String, Object> item) {
                return item.entrySet().stream()
                    .anyMatch(
                        e -> {
                          if (e.getValue() instanceof Number || e.getValue() instanceof String) {
                            try {
                              var value = Long.parseLong(e.getValue().toString());
                              return keyPattern.matcher(e.getKey()).matches()
                                  && value == Long.parseLong(valueString);
                            } catch (NumberFormatException ignored) {
                              // swallow
                            }
                          }
                          if (e.getValue() instanceof DataSize) {
                            var size = ((DataSize) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && size.equals(DataSize.of(valueString));
                          }
                          if (e.getValue() instanceof LocalDateTime) {
                            var time = ((LocalDateTime) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && time.compareTo(LocalDateTime.parse(valueString)) == 0;
                          }
                          return false;
                        });
              }
            };

    BiFunction<Pattern, String, Query> createLargePredicate =
        (keyPattern, valueString) ->
            new Query() {
              @Override
              public boolean required(String key) {
                return keyPattern.matcher(key).matches();
              }

              @Override
              public boolean required(Map<String, Object> item) {
                return item.entrySet().stream()
                    .anyMatch(
                        e -> {
                          if (e.getValue() instanceof Number || e.getValue() instanceof String) {
                            try {
                              var value = Long.parseLong(e.getValue().toString());
                              return keyPattern.matcher(e.getKey()).matches()
                                  && value > Long.parseLong(valueString);
                            } catch (NumberFormatException ignored) {
                              // swallow
                            }
                          }
                          if (e.getValue() instanceof DataSize) {
                            var size = ((DataSize) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && size.greaterThan(DataSize.of(valueString));
                          }
                          if (e.getValue() instanceof LocalDateTime) {
                            var time = ((LocalDateTime) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && time.compareTo(LocalDateTime.parse(valueString)) > 0;
                          }
                          return false;
                        });
              }
            };

    var queries = new ArrayList<Query>();
    var ss = predicateString.trim().split(">=");
    if (ss.length == 2) {
      queries.add(createLargePredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      queries.add(createEqualPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
    }
    ss = predicateString.trim().split("<=");
    if (queries.isEmpty() && ss.length == 2) {
      queries.add(createSmallPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      queries.add(createEqualPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
    }
    ss = predicateString.split("==");
    if (queries.isEmpty() && ss.length == 2) {
      queries.add(createEqualPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
    }
    ss = predicateString.trim().split(">");
    if (queries.isEmpty() && ss.length == 2) {
      queries.add(createLargePredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
    }
    ss = predicateString.split("<");
    if (queries.isEmpty() && ss.length == 2) {
      queries.add(createSmallPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
    }
    if (queries.isEmpty()) return Optional.empty();
    return Optional.of(
        new Query() {
          @Override
          public boolean required(String key) {
            return queries.stream().anyMatch(q -> q.required(key));
          }

          @Override
          public boolean required(Map<String, Object> item) {
            return queries.stream().anyMatch(p -> p.required(item));
          }
        });
  }

  private static Optional<Query> forEmpty(String predicateString) {
    if (predicateString == null || predicateString.isBlank()) return Optional.empty();
    if (!predicateString.endsWith("=")) return Optional.empty();
    if (predicateString.endsWith("=")
        && (predicateString.endsWith(">=")
            || predicateString.endsWith("==")
            || predicateString.endsWith("<="))) return Optional.empty();

    var keyPattern =
        Utils.wildcardToPattern(predicateString.substring(0, predicateString.length() - 1).trim());
    return Optional.of(
        new Query() {
          @Override
          public boolean required(String key) {
            return keyPattern.matcher(key).matches();
          }

          @Override
          public boolean required(Map<String, Object> item) {
            return item.entrySet().stream()
                .noneMatch(
                    entry ->
                        keyPattern.matcher(entry.getKey()).matches()
                            && !entry.getValue().toString().isBlank());
          }
        });
  }

  static Optional<Query> forString(String predicateString) {
    if (predicateString == null || predicateString.isBlank()) return Optional.empty();
    if (predicateString.contains("==")
        || predicateString.contains("<=")
        || predicateString.contains(">=")) return Optional.empty();
    var ss = predicateString.trim().split("=");
    if (ss.length != 2) return Optional.empty();
    var keyPattern = Utils.wildcardToPattern(ss[0].trim());
    var valuePattern = Utils.wildcardToPattern(ss[1].trim());
    return Optional.of(
        new Query() {
          @Override
          public boolean required(String key) {
            return keyPattern.matcher(key).matches();
          }

          @Override
          public boolean required(Map<String, Object> item) {
            return item.entrySet().stream()
                .anyMatch(
                    entry -> {
                      if (!keyPattern.matcher(entry.getKey()).matches()) return false;
                      var string = entry.getValue().toString();
                      if (string.isBlank()) return false;
                      return valuePattern.matcher(string).matches();
                    });
          }
        });
  }
}
