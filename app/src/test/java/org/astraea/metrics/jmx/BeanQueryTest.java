package org.astraea.metrics.jmx;

import java.util.Map;
import java.util.stream.Stream;
import javax.management.MalformedObjectNameException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BeanQueryTest {

  private static Stream<Arguments> dataset_objectNameIsCorrect() {
    return Stream.of(
        Arguments.of("org.example", Map.of("type", "testing")),
        Arguments.of("org.example", Map.of("type", "testing", "favorite_band", "5566")),
        Arguments.of("org.example", Map.of("k1", "v1", "k2", "v2", "k3", "v3")),
        Arguments.of("org.example", Map.of("k1", "A*", "k2", "A?", "k3", "*A")));
  }

  @ParameterizedTest
  @MethodSource("dataset_objectNameIsCorrect")
  void objectNameIsCorrect(String domainName, Map<String, String> properties)
      throws MalformedObjectNameException {
    BeanQuery sut = new BeanQuery(domainName, properties);

    Assertions.assertEquals("org.example", TestUtility.getDomainName(sut));
    Assertions.assertEquals(properties, TestUtility.getPropertyList(sut));
  }
}
