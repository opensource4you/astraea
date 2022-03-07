package org.astraea.argument;

import java.nio.file.FileSystems;
import java.nio.file.Path;

public class PathField extends Field<Path> {
  @Override
  public Path convert(String value) {
    return FileSystems.getDefault().getPath(value);
  }
}
