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
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;

public final class Queries {

  //  static Function<Map<String, Object>, Map<String, Object>>

  public static Predicate<Map<String, Object>> predicate(final String text) {
    if (text == null || text.isBlank()) return ignored -> true;
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

    var predicates =
        subStrings.stream()
            .map(String::trim)
            .flatMap(
                string ->
                    Stream.of(
                        forNumber(string).stream(),
                        forNonexistent(string).stream(),
                        forString(string).stream()))
            .flatMap(s -> s)
            .collect(Collectors.toList());

    return item -> {
      Boolean match = null;
      for (var index = 0; index != predicates.size(); ++index) {
        if (match == null) match = predicates.get(index).test(item);
        else {
          switch (ops.get(index - 1)) {
            case "&&":
              match = match && predicates.get(index).test(item);
              break;
            case "||":
              match = match || predicates.get(index).test(item);
              break;
            default:
              throw new IllegalArgumentException("unsupported op: " + ops.get(index - 1));
          }
        }
      }
      return match == null || match;
    };
  }

  private static Optional<Predicate<Map<String, Object>>> forNumber(String predicateString) {
    if (predicateString == null || predicateString.isBlank()) return Optional.empty();
    BiFunction<Pattern, String, Predicate<Map<String, Object>>> createSmallPredicate =
        (keyPattern, valueString) ->
            item ->
                item.entrySet().stream()
                    .anyMatch(
                        e -> {
                          if (e.getValue() instanceof Number) {
                            var value = ((Number) e.getValue()).longValue();
                            return keyPattern.matcher(e.getKey()).matches()
                                && value < Long.parseLong(valueString);
                          }
                          if (e.getValue() instanceof DataSize) {
                            var size = ((DataSize) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && size.smallerThan(new DataSize.Field().convert(valueString));
                          }
                          if (e.getValue() instanceof LocalDateTime) {
                            var time = ((LocalDateTime) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && time.compareTo(LocalDateTime.parse(valueString)) < 0;
                          }
                          return false;
                        });

    BiFunction<Pattern, String, Predicate<Map<String, Object>>> createEqualPredicate =
        (keyPattern, valueString) ->
            item ->
                item.entrySet().stream()
                    .anyMatch(
                        e -> {
                          if (e.getValue() instanceof Number) {
                            var value = ((Number) e.getValue()).longValue();
                            return keyPattern.matcher(e.getKey()).matches()
                                && value == Long.parseLong(valueString);
                          }
                          if (e.getValue() instanceof DataSize) {
                            var size = ((DataSize) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && size.equals(new DataSize.Field().convert(valueString));
                          }
                          if (e.getValue() instanceof LocalDateTime) {
                            var time = ((LocalDateTime) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && time.compareTo(LocalDateTime.parse(valueString)) == 0;
                          }
                          return false;
                        });

    BiFunction<Pattern, String, Predicate<Map<String, Object>>> createLargePredicate =
        (keyPattern, valueString) ->
            item ->
                item.entrySet().stream()
                    .anyMatch(
                        e -> {
                          if (e.getValue() instanceof Number) {
                            var value = ((Number) e.getValue()).longValue();
                            return keyPattern.matcher(e.getKey()).matches()
                                && value > Long.parseLong(valueString);
                          }
                          if (e.getValue() instanceof DataSize) {
                            var size = ((DataSize) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && size.greaterThan(new DataSize.Field().convert(valueString));
                          }
                          if (e.getValue() instanceof LocalDateTime) {
                            var time = ((LocalDateTime) e.getValue());
                            return keyPattern.matcher(e.getKey()).matches()
                                && time.compareTo(LocalDateTime.parse(valueString)) > 0;
                          }
                          return false;
                        });
    var ss = predicateString.trim().split(">=");
    if (ss.length == 2) {
      var ps =
          Stream.of(
              createLargePredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()),
              createEqualPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      return Optional.of(item -> ps.anyMatch(p -> p.test(item)));
    }
    ss = predicateString.trim().split("<=");
    if (ss.length == 2) {
      var ps =
          Stream.of(
              createSmallPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()),
              createEqualPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      return Optional.of(item -> ps.anyMatch(p -> p.test(item)));
    }
    ss = predicateString.split("==");
    if (ss.length == 2) {
      var ps =
          Stream.of(
              createEqualPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      return Optional.of(item -> ps.anyMatch(p -> p.test(item)));
    }
    ss = predicateString.trim().split(">");
    if (ss.length == 2) {
      var ps =
          Stream.of(
              createLargePredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      return Optional.of(item -> ps.anyMatch(p -> p.test(item)));
    }
    ss = predicateString.split("<");
    if (ss.length == 2) {
      var ps =
          Stream.of(
              createSmallPredicate.apply(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim()));
      return Optional.of(item -> ps.anyMatch(p -> p.test(item)));
    }
    return Optional.empty();
  }

  private static Optional<Predicate<Map<String, Object>>> forNonexistent(String predicateString) {
    if (predicateString == null || predicateString.isBlank()) return Optional.empty();
    if (!predicateString.endsWith("=")) return Optional.empty();
    if (predicateString.endsWith("=")
        && (predicateString.endsWith(">=")
            || predicateString.endsWith("==")
            || predicateString.endsWith("<="))) return Optional.empty();

    var pattern =
        Utils.wildcardToPattern(predicateString.substring(0, predicateString.length() - 1).trim());
    return Optional.of(
        item ->
            item.entrySet().stream()
                .noneMatch(
                    entry ->
                        pattern.matcher(entry.getKey()).matches()
                            && !entry.getValue().toString().isBlank()));
  }

  private static Optional<Predicate<Map<String, Object>>> forString(String predicateString) {
    if (predicateString == null || predicateString.isBlank()) return Optional.empty();
    if (predicateString.contains("==")
        || predicateString.contains("<=")
        || predicateString.contains(">=")) return Optional.empty();
    var ss = predicateString.trim().split("=");
    if (ss.length != 2) return Optional.empty();
    var keyPattern = Utils.wildcardToPattern(ss[0].trim());
    var valuePattern = Utils.wildcardToPattern(ss[1].trim());
    return Optional.of(
        item ->
            item.entrySet().stream()
                .anyMatch(
                    entry ->
                        keyPattern.matcher(entry.getKey()).matches()
                            && valuePattern.matcher(entry.getValue().toString()).matches()));
  }
}
