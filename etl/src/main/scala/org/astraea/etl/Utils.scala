package org.astraea.etl

import java.awt.geom.IllegalPathStateException
import java.io.File

object Utils {
  def ToDirectory(path: String): File = {
    val file = new File(path)
    if (!file.isDirectory) {
      throw new IllegalPathStateException(
        path + "is not a folder." + "The path should be a folder."
      )
    }
    file
  }

  def isPositive(number: Int): Boolean = {
    number > 0
  }
}
