package org.astraea.etl

import scopt.OParser

/** This argument defines the common property used by ETL. */
object ArgumentETL {

  /** @param args
   * Command line arguments that are put into main function.
   * @return
   * Command line arguments.
   */
  def parseArgument(args: Array[String]): Option[Argument] = {
    val builder = OParser.builder[Argument]
    val parser = OParser.sequence(
      programName("spark"),
      head("Welcome to Astraea ETL."),
      opt[String]("prop.file")
        .required()
        .action((x, c) => c.copy(propFile = x))
        .text("You must set prop file."),
      help("help").text("prints this usage text")
    )

    OParser.parse(parser, args, Argument())
  }
}
