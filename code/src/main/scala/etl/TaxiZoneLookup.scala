package etl

import etl.Utils.{keyCleanOutput, keySource, parseOpts}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object TaxiZoneLookup {
  case class TaxiZoneLookupTable(location_id: Int, zone: String, borough: String)

  def loadRawData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(path)
  }

  def cleanRawData(spark: SparkSession, rawDF: DataFrame): Dataset[TaxiZoneLookupTable] = {
    import spark.implicits._

    rawDF.drop("service_zone")
      .select("LocationID", "Zone", "Borough")
      .withColumnRenamed("LocationID", "location_id")
      .withColumnRenamed("Borough", "borough")
      .withColumnRenamed("Zone", "zone")
      .as[TaxiZoneLookupTable]
      .filter(r => r.borough != "Unknown")
      .coalesce(1)
  }

  def saveCleanData(cleanDS: Dataset[TaxiZoneLookupTable], path: String): Unit = {
    val boroughs = cleanDS.select("borough")
      .distinct
      .collect
      .map(_(0).asInstanceOf[String])

    boroughs.foreach(b =>
      cleanDS.filter(r => r.borough == b)
        .write
        .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
        .option("header", true)
        .csv(f"$path/$b")
    )
  }

  def loadCleanData(spark: SparkSession, path: String, borough: String = "Manhattan"): Dataset[TaxiZoneLookupTable] = {
    import spark.implicits._

    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(f"$path/$borough")
      .as[TaxiZoneLookupTable]
  }

  def main(args: Array[String]): Unit = {
    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]

    val spark = SparkSession.builder().appName("ETLTaxiZoneLookup").getOrCreate()

    val rawDF = loadRawData(spark, sourcePath)
    val cleanDS = cleanRawData(spark, rawDF)
    saveCleanData(cleanDS, cleanOutputPath)

    //loadCleanData(spark, cleanOutputPath).show()
  }
}
