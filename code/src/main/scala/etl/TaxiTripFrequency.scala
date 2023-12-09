package etl
import etl.Utils.{keyCleanOutput, keySource, loadintermediateData, parseOpts}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.rdd.RDD

object TaxiTripFrequency {
  val spark = SparkSession.builder().appName("TaxiTripFrequency_ETL").getOrCreate()
  val schema = StructType(Array(StructField("pulocationID", LongType, true), StructField("dolocationID", LongType, true)))

  def cleanRawData(rawDF: DataFrame,intermediateDF: DataFrame): DataFrame = {
    val isolatedLocations = List(103, 104, 105, 153, 194, 202)
    val manhattanLocationIDs = intermediateDF.select("location_id").distinct().collect().map(_.getInt(0))

    // pick-up and drop-off locations are the same.
    val uniquePUDO = rawDF.filter(col("pulocationID") =!= col("dolocationID"))

    // isolated location
    val nonIsolatedDF = uniquePUDO.filter(!col("pulocationID").isin(isolatedLocations: _*) && !col("dolocationID").isin(isolatedLocations: _*))

    // in manhattan borough
    val manhattanDF = nonIsolatedDF.filter(col("pulocationID").isin(manhattanLocationIDs: _*) && col("dolocationID").isin(manhattanLocationIDs: _*))

    // count the frequency of location pairs
    val frequencyDF = manhattanDF.groupBy("pulocationID", "dolocationID").agg(count("*") as "frequency")

    //convert into pair DF
    frequencyDF.withColumn("PUDOPair", struct("pulocationID", "dolocationID")).select("PUDOPair", "count(1)").withColumnRenamed("count(1)", "Frequency")

  }
  def saveCleanData(freqDF: DataFrame, cleanOutputPath: String,year: Int, cab: String): Unit = {
    freqDF.repartition(100).write.mode(SaveMode.Overwrite).parquet(s"$cleanOutputPath/${cab}/${year}/${cab}_intermediate_${year}.parquet")
  }

//  def loadCleanSavedData(spark: SparkSession, path: String, cab: String, year:Int): DataFrame = {
//    loadintermediateData(spark,path,year,cab)
//  }

  def main(args: Array[String]): Unit = {
    val years = Seq(2020, 2021, 2022, 2023)
    val cabs = Seq("fhv","fhvhv","yellow","green")
    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]
    val intermediatePath = "/user/wl2484_nyu_edu/project/data/intermediate/location_neighbors_distance/Manhattan/part-00000-7b78c933-8e72-43de-ad42-0ea0202969e4-c000.csv"
    val intermediateDF= spark.read.option("header", true).option("inferSchema", true).csv(intermediatePath)

    var combinedRDD: RDD[Row] = spark.sparkContext.emptyRDD[Row]

    for (cab <- cabs; year <- years) {
      val rawDF = loadintermediateData(spark, sourcePath,year, cab)
      // cleaning
      val freqDF = cleanRawData(rawDF, intermediateDF)
      saveCleanData(freqDF, cleanOutputPath,year,cab)
    }
  }
}

