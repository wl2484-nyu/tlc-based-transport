package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  val keySource = "source"
  val optSource = "--" + keySource
  val keyCleanOutput = "clean-output"
  val optCleanOutput = "--" + keyCleanOutput
  val keyProfileOutput = "profile-output"
  val optProfileOutput = "--" + keyProfileOutput
  val keyIntermediateOutput = "intermediate-output"
  val optIntermediateOutput = "--" + keyIntermediateOutput

  val keyNeighborsDistanceInput = "ns-dis-input"
  val optNeighborsDistanceInput = "--" + keyNeighborsDistanceInput

  def parseOpts(map: Map[String, Any], list: List[String]): Map[String, Any] = {
    list match {
      case Nil => map
      case `optSource` :: value :: tail =>
        parseOpts(map ++ Map(keySource -> value), tail)
      case `optCleanOutput` :: value :: tail =>
        parseOpts(map ++ Map(keyCleanOutput -> value), tail)
      case `optProfileOutput` :: value :: tail =>
        parseOpts(map ++ Map(keyProfileOutput -> value), tail)
      case `optIntermediateOutput` :: value :: tail =>
        parseOpts(map ++ Map(keyIntermediateOutput -> value), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        map
    }
  }

  def parseMainOpts(map: Map[String, Any], list: List[String]): Map[String, Any] = {
    list match {
      case Nil => map
      case `optNeighborsDistanceInput` :: value :: tail =>
        parseOpts(map ++ Map(keyNeighborsDistanceInput -> value), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        map
    }
  }

  def loadRawDataCSV(spark: SparkSession, path: String, headers: Boolean = true, inferSchema: Boolean = true,
                     delimiter: String = ","): DataFrame = {
    spark.read
      .option("header", headers)
      .option("inferSchema", inferSchema)
      .option("delimiter", delimiter)
      .csv(path)
  }
}
