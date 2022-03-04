package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

public class PathField extends Field<Path> {
  @Override
  public Path convert(String value) {
    try {
      return FileSystems.getDefault().getPath(value);
    } catch (InvalidPathException ipe) {
      throw new ParameterException(ipe);
    }
  }
}
