package etl

import etl.Utils.{keyCleanOutput, keySource, loadRawTLCDataParquet, getNumericColumnLowerAndUpperBound, parseOpts}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object TaxiGreen {
  val spark = SparkSession.builder().appName("tlcGreenTaxiETL").getOrCreate()
  val schema = StructType(Array(StructField("pulocationID", LongType, true), StructField("dolocationID", LongType, true)))

  def saveCleanData(resultDF: DataFrame, cleanOutputPath: String): Unit = {
    resultDF.write.mode(SaveMode.Overwrite).parquet(s"$cleanOutputPath/merged_green_cleaned_data.parquet")
  }

  def loadCleanData(path: String): DataFrame = {
    spark.read.parquet(f"$path/merged_green_cleaned_data.parquet")
  }

  def cleanRawDataYellow(rawDF: DataFrame): DataFrame = {

    // Keep non-null entries in these columns
    val nonNullDF = rawDF.filter(
      col("PUlocationID").isNotNull &&
        col("DOlocationID").isNotNull &&
        col("total_amount").isNotNull &&
        col("fare_amount").isNotNull &&
        col("payment_type").isNotNull &&
        col("RatecodeID").isNotNull)

    // Rename columns
    val newColumnNames = Map(
      "lpep_dropoff_datetime" -> "dropoff_datetime",
      "lpep_pickup_datetime" -> "pickup_datetime",
      "PUlocationID" -> "pulocationID",
      "DOlocationID" -> "dolocationID"
    )

    val renamedDF = newColumnNames.foldLeft(nonNullDF) {
      case (accDF, (oldName, newName)) => accDF.withColumnRenamed(oldName, newName)
    }

    // Compute valid ranges of values for columns
    val validPaymentTypes = Seq(1, 2, 3)
    val validRateCodeIds = Seq(1, 5, 6)
    val validTotalAmount = getNumericColumnLowerAndUpperBound(rawDF, "total_amount")
    val totalAmountLower = validTotalAmount._1
    val totalAmountUpper = validTotalAmount._2
    val pattern = """(\d{4}-\d{2})""".r

    // Add month-date from data and compare it with month-date extracted from data file name
    val transformedDF = renamedDF.withColumn("year_month", regexp_extract(col("file_path"), pattern.toString, 1)).withColumn("data_year_month", concat_ws("-", year(col("pickup_datetime")), format_string("%02d", month(col("pickup_datetime")))))

    // Remove rows which follow rules
    val filteredDF = transformedDF.filter(col("passenger_count") =!= 0).filter(col("pulocationID") =!= col("dolocationID")).filter(col("data_year_month") === col("year_month")).filter(col("payment_type").isin(validPaymentTypes: _*)).filter(col("RatecodeID").isin(validRateCodeIds: _*)).filter(col("fare_amount") =!= 0).filter(col("total_amount").between(totalAmountLower, totalAmountUpper))

    // Impute: cast to Long
    val castDf = filteredDF.withColumn("pulocationID", col("pulocationID").cast(LongType)).withColumn("dolocationID", col("dolocationID").cast(LongType))
    castDf.select("pulocationID", "dolocationID").coalesce(1)
  }

  def main(args: Array[String]): Unit = {
    val months = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")

    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]

    val filePaths2020 = for (m <- months.slice(9, 12)) yield s"$sourcePath/2020/$m/green_tripdata_2020-${m}"
    val filePaths2021 = for (m <- months) yield s"$sourcePath/2021/${m}/green_tripdata_2021-${m}"
    val filePaths2022 = for (m <- months) yield s"$sourcePath/2022/${m}/green_tripdata_2022-${m}"
    val filePaths2023 = for (m <- months.slice(0, 9)) yield s"$sourcePath/2023/${m}/green_tripdata_2023-${m}"

    val filePaths = filePaths2020 ++ filePaths2021 ++ filePaths2022 ++ filePaths2023

    var resultDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    for (filePath <- filePaths) {
      val rawDF = loadRawTLCDataParquet(spark, filePath)
      val transformedDF = rawDF.withColumn("file_path", lit(filePath))
      val cleanDF = cleanRawDataYellow(transformedDF)
      resultDF = resultDF.union(cleanDF)
    }

    saveCleanData(resultDF, cleanOutputPath)
    loadCleanData(cleanOutputPath).show()

  }
}