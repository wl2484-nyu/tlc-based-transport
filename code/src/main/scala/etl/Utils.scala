package etl

import scala.sys.exit

object Utils {
  val keySource = "source"
  val optSource = "--" + keySource
  val keyCleanOutput = "clean-output"
  val optCleanOutput = "--" + keyCleanOutput

  def parseOpts(map: Map[String, Any], list: List[String]): Map[String, Any] = {
    list match {
      case Nil => map
      case `optSource` :: value :: tail =>
        parseOpts(map ++ Map(keySource -> value), tail)
      case `optCleanOutput` :: value :: tail =>
        parseOpts(map ++ Map(keyCleanOutput -> value), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        map
    }
  }
}
