package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringMapFieldTest {

  @Test
  void testStringInMap() {
    var field = new StringMapField();
    var result = field.convert("k0=v0,k1=v1");
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals("v0", result.get("k0"));
    Assertions.assertEquals("v1", result.get("k1"));
  }

  @Test
  void testNonMap() {
    var field = new StringMapField();
    Assertions.assertThrows(ParameterException.class, () -> field.validate("a", "bb"));
    Assertions.assertThrows(ParameterException.class, () -> field.validate("a", "bb="));
  }
}
