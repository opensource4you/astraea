package org.astraea.partitioner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Configuration {

  static Configuration of(Map<String, ?> configs) {
    return new Configuration() {
      @Override
      public String string(String key) {
        return Objects.requireNonNull(configs.get(key), key + " is nonexistent").toString();
      }

      @Override
      public List<String> list(String key, String separator) {
        return Arrays.asList(string(key).split(separator));
      }

      @Override
      public <K, V> Map<K, V> map(
          String key,
          String listSeparator,
          String mapSeparator,
          Function<String, K> keyConverter,
          Function<String, V> valueConverter) {
        Function<String, Map.Entry<K, V>> split =
            s -> {
              var items = s.split(mapSeparator);
              if (items.length != 2)
                throw new IllegalArgumentException(
                    "the value: " + s + " is using incorrect separator");
              return Map.entry(keyConverter.apply(items[0]), valueConverter.apply(items[1]));
            };
        return list(key, listSeparator).stream()
            .map(split)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    };
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return string value. never null
   */
  String string(String key);

  /**
   * @param key the key whose associated value is to be returned
   * @param separator to split string to multiple strings
   * @return string list. never null
   */
  List<String> list(String key, String separator);

  /**
   * @param key the key whose associated value is to be returned
   * @param listSeparator to split string to multiple strings
   * @param mapSeparator to split multiple strings to map
   * @return string map. never null
   */
  default Map<String, String> map(String key, String listSeparator, String mapSeparator) {
    return map(key, listSeparator, mapSeparator, s -> s);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param listSeparator to split string to multiple strings
   * @param mapSeparator to split multiple strings to map
   * @param valueConverter used to convert string to specify type
   * @return string map. never null
   */
  default <T> Map<String, T> map(
      String key, String listSeparator, String mapSeparator, Function<String, T> valueConverter) {
    return map(key, listSeparator, mapSeparator, s -> s, valueConverter);
  }

  <K, V> Map<K, V> map(
      String key,
      String listSeparator,
      String mapSeparator,
      Function<String, K> keyConverter,
      Function<String, V> valueConverter);
}
