package org.astraea.app.argument;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FieldTest {

  @Test
  void testNullAndEmptyValue() {
    var f =
        new Field<String>() {
          @Override
          public String convert(String value) {
            return null;
          }
        };
    Assertions.assertThrows(ParameterException.class, () -> f.validate("a", ""));
    Assertions.assertThrows(ParameterException.class, () -> f.validate("a", null));
    Assertions.assertThrows(ParameterException.class, () -> f.validate("a", "   "));
  }
}
