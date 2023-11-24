package profile

import org.apache.spark.sql.{DataFrame, SparkSession}

object ProfileTaxiZoneLookup {
  val input_path = "project/data/clean/tlc/zone_lookup"
  val output_path = "project/data/profile/tlc/zone_lookup"

  def profile(df: DataFrame): DataFrame = {
    // TODO: calculate location count per borough
    df
  }

  def save(df: DataFrame, path: String): Unit = {
    // TODO: save
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ProfileTaxiZoneLookup").getOrCreate()

    val df = spark.read.option("header", true).option("inferSchema", true).csv(input_path)
    val profile_df = profile(df)
    save(profile_df, output_path)
  }
}
