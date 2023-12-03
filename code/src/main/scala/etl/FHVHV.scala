package etl

import etl.Utils.{keyCleanOutput, keyProfileOutput, keySource, loadRawDataParquet, parseOpts}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, LongType}

object FHVHV {
  val spark = SparkSession.builder().appName("FHVHV_ETL").getOrCreate()
  val schema = StructType(Array(StructField("pulocationID", LongType, true), StructField("dolocationID", LongType, true)))

  def cleanRawData(rawDF: DataFrame): DataFrame = {
    import spark.implicits._
    val nonNullDf = rawDF.filter(col("PUlocationID").isNotNull && col("DOlocationID").isNotNull)
    val castDf = nonNullDf.withColumn("pulocationID", col("PUlocationID").cast(LongType)).withColumn("dolocationID", col("DOlocationID").cast(LongType))
    val dfFinal = castDf.drop("PUlocationID", "DOlocationID")
    dfFinal.select("pulocationID", "dolocationID").coalesce(1)
  }

  def saveCleanData(resultDF: DataFrame, cleanOutputPath: String): Unit = {
    resultDF.write.mode(SaveMode.Overwrite).parquet(s"$cleanOutputPath/merged_fhvhv_cleaned_data.parquet")
  }

  def loadCleanData(path: String): DataFrame = {
    import spark.implicits._
    spark.read.parquet(f"$path/merged_fhvhv_cleaned_data.parquet")
  }

  def main(args: Array[String]): Unit = {
    val years = Seq(2020, 2021, 2022, 2023)
    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]
    val profileOutputPath = options(keyProfileOutput).asInstanceOf[String]

    var resultDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    for (year <- years) {
      val rawDF = loadRawDataParquet(spark, sourcePath, year)
      // cleaning
      val cleanDS = cleanRawData(rawDF)
      resultDF = resultDF.union(cleanDS)
      // testing clean data loading by borough
    }
    saveCleanData(resultDF, cleanOutputPath)
    loadCleanData(cleanOutputPath).show()
  }
}

