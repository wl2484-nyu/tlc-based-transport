package etl

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CleanTaxiZoneLookup {
  val inputPath = "project/data/source/tlc/zone_lookup"
  val outputPath = "project/data/clean/tlc/zone_lookup"

  case class TaxiZoneLookupTable(location_id: Int, borough: String, zone: String)

  def clean(spark: SparkSession, df: DataFrame): Dataset[TaxiZoneLookupTable] = {
    import spark.implicits._

    df.drop("service_zone")
      .withColumnRenamed("LocationID", "location_id")
      .withColumnRenamed("Borough", "borough")
      .withColumnRenamed("Zone", "zone")
      .as[TaxiZoneLookupTable]
      .filter(r => r.borough != "Unknown")
  }

  def save(ds: Dataset[TaxiZoneLookupTable], path: String): Unit = {
    val sorted_ds = ds.repartition(col("borough"))
      .sortWithinPartitions("location_id")

    sorted_ds.select("borough")
      .distinct
      .collect
      .map(_(0).asInstanceOf[String])
      .foreach(b =>
        sorted_ds.filter(r => r.borough == b)
          .write
          .option("header", true)
          .csv(f"$outputPath/$b")
      )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CleanTaxiZoneLookup").getOrCreate()

    val raw_df = spark.read.option("header", true).option("inferSchema", true).csv(inputPath)
    val clean_df = clean(spark, raw_df)
    save(clean_df, outputPath)
  }
}
