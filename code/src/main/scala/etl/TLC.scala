package etl

import etl.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp

object TLC {
  case class TaxiTrip(tlc_type: String, pu_datetime: Timestamp, do_datetime: Timestamp, pu_location_id: Long, do_location_id: Long)

  val dataColsByType = Seq(
    ("fhv", Seq("pickup_datetime", "dropOff_datetime", "PUlocationID", "DOlocationID")),
    ("fhvhv", Seq("pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID")),
    ("green", Seq("lpep_pickup_datetime", "lpep_dropoff_datetime", "PUlocationID", "DOlocationID")),
    ("yellow", Seq("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"))
  )
  val newColNames = Seq("tlc_type", "pu_datetime", "do_datetime", "pu_location_id", "do_location_id")

  def loadAndCleanRawData(spark: SparkSession, basePath: String): Dataset[TaxiTrip] = {
    import spark.implicits._

    def cleanRawData(rawDF: DataFrame, tlcType: String, oldColNames: Seq[String]): Dataset[TaxiTrip] = {
      rawDF.select(oldColNames.head, oldColNames.tail: _*)
        .na.drop()  // drop rows with null value in any selected columns
        .withColumn(newColNames(0), lit(tlcType))
        .withColumnRenamed(oldColNames(0), newColNames(1))
        .withColumnRenamed(oldColNames(1), newColNames(2))
        .withColumnRenamed(oldColNames(2), newColNames(3))
        .withColumnRenamed(oldColNames(3), newColNames(4))
        .withColumn("pu_location_id", $"pu_location_id".cast(LongType))
        .withColumn("do_location_id", $"do_location_id".cast(LongType))
        .select(newColNames.head, newColNames.tail: _*)
        .as[TaxiTrip]
    }

    dataColsByType.map {
      case (tlcType, oldColNames) => {
        val by202301DS = cleanRawData(spark.read.parquet(f"$basePath/$tlcType/202[0-2]/*/*", f"$basePath/$tlcType/2023/01/*"), tlcType, oldColNames)
        val from202302DS = cleanRawData(spark.read.parquet(f"$basePath/$tlcType/2023/0[2-9]/*"), tlcType, oldColNames)
        by202301DS.union(from202302DS)
      }
    }.reduce((x, y) => x union y)
  }

  def main(args: Array[String]): Unit = {
    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val profileOutputPath = options(keyProfileOutput).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]

    val spark = SparkSession.builder().appName("TLC_ETL").getOrCreate()
    val ds = loadAndCleanRawData(spark, sourcePath)
  }
}
