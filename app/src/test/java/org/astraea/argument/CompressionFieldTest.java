package org.astraea.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.stream.Stream;
import org.astraea.admin.Compression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompressionFieldTest {

  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        converter = CompressionField.class,
        validateWith = CompressionField.class)
    public Compression value;
  }

  @Test
  void testConversion() {
    var arg = new CompressionField();
    Stream.of(Compression.values())
        .forEach(type -> Assertions.assertEquals(type, arg.convert(type.name())));
    Assertions.assertThrows(ParameterException.class, () -> arg.convert("aaa"));
  }

  @Test
  void testParse() {
    var arg =
        org.astraea.argument.Argument.parse(new FakeParameter(), new String[] {"--field", "gzip"});
    Assertions.assertEquals(Compression.GZIP, arg.value);
  }
}
