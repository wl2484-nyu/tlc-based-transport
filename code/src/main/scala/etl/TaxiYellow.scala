package etl

import etl.Utils.{keyCleanOutput, keyProfileOutput, keySource, loadRawDataParquet, parseOpts}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, LongType}

object TaxiYellow {
  val spark = SparkSession.builder().appName("TaxiYellowETL").getOrCreate()
  val schema = StructType(Array(StructField("pulocationID", LongType, true), StructField("dolocationID", LongType, true)))

  def cleanRawData(rawDF: DataFrame): Unit = {
    {}
  }

  def saveCleanData(resultDF: DataFrame, cleanOutputPath: String): Unit = {
    {}
  }

  def loadCleanData(path: String): Unit = {
    {}
  }

  def main(args: Array[String]): Unit = {
    {}
  }
}

