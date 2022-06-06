package org.astraea.app.argument;

import com.beust.jcommander.ParameterException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.record.CompressionType;
import org.astraea.app.admin.Compression;

public class CompressionField extends Field<Compression> {
  /**
   * @param value Name of compression type. Accept lower-case name only ("none", "gzip", "snappy",
   *     "lz4", "zstd").
   */
  @Override
  public Compression convert(String value) {
    try {
      // `CompressionType#forName` accept lower-case name only.
      return Compression.of(value);
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
  protected void check(String name, String value) throws ParameterException {
    convert(value);
  }
}
