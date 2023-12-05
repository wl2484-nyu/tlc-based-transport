package etl
import etl.Utils.{keyCleanOutput, keySource, loadRawDataParquet, parseOpts}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, LongType}

object FHV {
  val spark = SparkSession.builder().appName("FHV_ETL").getOrCreate()
  val schema = StructType(Array(StructField("pulocationID", LongType, true), StructField("dolocationID", LongType, true)))

  def cleanRawData(rawDF: DataFrame): DataFrame = {
    val nonNullDf = rawDF.filter(col("PUlocationID").isNotNull && col("DOlocationID").isNotNull)
    val castDf = nonNullDf.withColumn("pulocationID", col("PUlocationID").cast(LongType)).withColumn("dolocationID", col("DOlocationID").cast(LongType))
    castDf.select("pulocationID", "dolocationID").coalesce(1)
  }

  def saveCleanData(resultDF: DataFrame, cleanOutputPath: String, year: Int): Unit = {
    resultDF.repartition(100).write.mode(SaveMode.Overwrite).parquet(s"$cleanOutputPath/cleaned_${year}.parquet")
  }

  def loadCleanData(path: String): DataFrame = {
    spark.read.parquet(f"$path/*.parquet")
  }

  def main(args: Array[String]): Unit = {
    val years = Seq(2020, 2021, 2022, 2023)
    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]

    for (year <- years) {
      val rawDF = loadRawDataParquet(spark, sourcePath, year)
      // cleaning
      val cleanDS = cleanRawData(rawDF)
      saveCleanData(cleanDS, cleanOutputPath,year)
    }
    loadCleanData(cleanOutputPath).show()
  }
}

