package profile

import org.apache.spark.sql.{DataFrame, SparkSession}

object ProfileTaxiZones {
  val input_path = "project/data/clean/tlc/zones"
  val output_path = "project/data/profile/tlc/zones"

  def profile(df: DataFrame): DataFrame = {
    // TODO: calculate (1) average number of boundary coordinates per location, (2) average latitude, (3) average longitude
    df
  }

  def save(df: DataFrame, path: String): Unit = {
    // TODO: save
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ProfileTaxiZones").getOrCreate()

    val df = spark.read.option("header", true).option("inferSchema", true).csv(input_path)
    val profile_df = profile(df)
    save(profile_df, output_path)
  }
}
