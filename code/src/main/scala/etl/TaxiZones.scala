package etl

import etl.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object TaxiZones {
  case class TaxiZoneWithBoundary(borough: String, location_id: Int, boundary: Array[Array[(Double, Double)]])

  def cleanRawData(spark: SparkSession, rawDF: DataFrame): Dataset[TaxiZoneWithBoundary] = {
    import spark.implicits._

    val pattern = """([1-9][0-9]*),MULTIPOLYGON \(\(\((.*)\)\)\),([a-zA-Z ]+)$""".r

    rawDF.select("LocationID", "the_geom", "borough")
      .withColumnRenamed("LocationID", "location_id")
      .map(r => {
        val rStr = r.mkString(",")
        val pattern(locationId, geom, borough) = rStr
        val boundary = geom.split("\\)\\), \\(\\(")
          .map(subRegionGeomStr => subRegionGeomStr.split(", ") // multiple coordinates separated by ", "
            .map(coordinateStr => {
              val coordinate = coordinateStr.split(" ").map(_.toDouble) // each coordinates consists of latitude and longitude separated by a whitespace
              (coordinate(0), coordinate(1)) // (latitude, longitude)
            }))
          .sortWith(_.length > _.length) // sort the sub-region boundaries by the number of coordinates within
        TaxiZoneWithBoundary(borough, locationId.toInt, boundary)
      })
  }

  def profileBoroughByLocationCount(cleanDS: Dataset[TaxiZoneWithBoundary], path: String): Unit = {
    cleanDS.groupBy("borough")
      .count()
      .coalesce(1)
      .withColumnRenamed("count", "location_count")
      .orderBy(desc("location_count"), asc("borough"))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .csv(f"$path/borough_location_count")
  }

  def profileBoroughLocationByBoundary(cleanDS: Dataset[TaxiZoneWithBoundary], path: String, boroughs: Seq[String]): Unit = {
    // define udfs
    val totalCoordinateCount: Array[Array[(Double, Double)]] => Int = _.map(_.length).sum
    val totalCoordinateCountUDF = udf(totalCoordinateCount)
    val maxSubRegionCoordinateCount: Array[Array[(Double, Double)]] => Int = _.map(_.length).max
    val maxSubRegionCoordinateCountUDF = udf(maxSubRegionCoordinateCount)
    val subRegionCoordinateCounts: Array[Array[(Double, Double)]] => String = _.map(_.length).mkString(" ")
    val subRegionCoordinateCountsUDF = udf(subRegionCoordinateCounts)

    val statsDS = cleanDS.withColumn("total_coordinate_count", totalCoordinateCountUDF(col("boundary")))
      .withColumn("total_sub_region_count", size(col("boundary")))
      .withColumn("max_sub_region_coordinate_count", maxSubRegionCoordinateCountUDF(col("boundary")))
      .withColumn("sub_region_coordinate_counts", subRegionCoordinateCountsUDF(col("boundary")))
      .drop("boundary")
      .orderBy(asc("borough"), desc("total_sub_region_count"))
      .coalesce(1)

    boroughs.foreach(b =>
      statsDS.filter(f"borough = '$b'")
        .write
        .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
        .option("header", true)
        .csv(f"$path/borough_location_boundary_stats/$b")
    )
  }

  def saveSummarizedCleanData(cleanDS: Dataset[TaxiZoneWithBoundary], path: String, boroughs: Seq[String]): Unit = {
    // define udfs
    val avgLatOfTheLargestSubRegion: Array[Array[(Double, Double)]] => Double = { boundary =>
      val (count, latSum) = boundary.head.foldLeft((0, 0.0)) {
        case (acc, coordinate) => (acc._1 + 1, acc._2 + coordinate._1)
      }
      latSum / count
    }
    val avgLatOfTheLargestSubRegionUDF = udf(avgLatOfTheLargestSubRegion)
    val avgLonOfTheLargestSubRegion: Array[Array[(Double, Double)]] => Double = { boundary =>
      val (count, latSum) = boundary.head.foldLeft((0, 0.0)) {
        case (acc, coordinate) => (acc._1 + 1, acc._2 + coordinate._2)
      }
      latSum / count
    }
    val avgLonOfTheLargestSubRegionUDF = udf(avgLonOfTheLargestSubRegion)

    val summarizedDS = cleanDS.withColumn("avg_lat", avgLatOfTheLargestSubRegionUDF(col("boundary")))
      .withColumn("avg_lon", avgLonOfTheLargestSubRegionUDF(col("boundary")))
      .drop("boundary")
      .orderBy(asc("borough"), asc("location_id"))
      .coalesce(1)

    boroughs.foreach(b =>
      summarizedDS.filter(f"borough = '$b'")
        .write
        .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
        .option("header", true)
        .csv(f"$path/$b")
    )
  }

  def main(args: Array[String]): Unit = {
    val options = parseOpts(Map(), args.toList)
    val sourcePath = options(keySource).asInstanceOf[String]
    val profileOutputPath = options(keyProfileOutput).asInstanceOf[String]
    val cleanOutputPath = options(keyCleanOutput).asInstanceOf[String]

    val spark = SparkSession.builder().appName("TaxiZonesETL").getOrCreate()
    val rawDF = loadRawDataCSV(spark, sourcePath)

    // cleaning
    val cleanDS = cleanRawData(spark, rawDF)

    // profiling
    val boroughs = cleanDS.select("borough")
      .distinct
      .collect
      .map(_ (0).asInstanceOf[String])
      .toSeq

    profileBoroughByLocationCount(cleanDS, profileOutputPath)
    profileBoroughLocationByBoundary(cleanDS, profileOutputPath, boroughs)

    // save summarized clean data
    saveSummarizedCleanData(cleanDS, cleanOutputPath, boroughs)
  }
}
