package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.record.CompressionType;

public class CompressionField implements NonEmptyField<CompressionType> {
  /**
   * @param value Name of compression type. Accept lower-case name only ("none", "gzip", "snappy",
   *     "lz4", "zstd").
   */
  @Override
  public CompressionType convert(String value) {
    try {
      // `CompressionType#forName` accept lower-case name only.
      return CompressionType.forName(value);
    } catch (IllegalArgumentException e) {
      throw new ParameterException(
          "the "
              + value
              + " is unsupported. The supported algorithms are "
              + Stream.of(CompressionType.values())
                  .map(CompressionType::name)
                  .collect(Collectors.joining(",")));
    }
  }

  @Override
  public void validate(String name, String value) throws ParameterException {
    NonEmptyField.super.validate(name, value);
    convert(value);
  }
}
