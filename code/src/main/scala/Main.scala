import etl.TLC.{TaxiTrip, loadCleanData}
import etl.TaxiZoneNeighboring.{getBoroughConnectedLocationList, getBoroughIsolatedLocationList, loadLocationNeighborsDistanceByBorough}
import etl.Utils._
import graph.{Dijkstra, WeightedEdge, WeightedGraph}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable


object Main {
  val borough = "Manhattan" // target borough

  case class TaxiTripFreq(pu_location_id: Long, do_location_id: Long, frequency: BigInt)

  case class TripPathFreq(frequency: BigInt, trip_path: String)

  case class TripPathCoverageCount(trip_path: String, coverage_count: BigInt)

  case class HumanReadableTripPathCoverageCount(route: String, coverage_count: BigInt)

  case class TopKHumanReadableRouteCoveragePercentage(route: String, coverage_percentage: Double)

  // step-1
  def buildZoneNeighboringGraph(spark: SparkSession, neighborsDistanceInputPath: String): WeightedGraph[Long] = {
    import spark.implicits._

    val zoneNeighborDisDS = loadLocationNeighborsDistanceByBorough(spark, neighborsDistanceInputPath, borough = borough)
    new WeightedGraph[Long](zoneNeighborDisDS.map(r =>
      (r.location_id, WeightedEdge(r.neighbor_location_id, r.distance))).rdd
      .groupByKey()
      .mapValues(_.toList)
      .collect
      .toMap)
  }

  // step-2
  def getTaxiTripFreqDS(spark: SparkSession, rawTrips: Dataset[TaxiTrip], conLocList: List[Long],
                        isoLocList: List[Long]): Dataset[TaxiTripFreq] = {
    import spark.implicits._

    rawTrips.select("pu_location_id", "do_location_id")
      .groupBy("pu_location_id", "do_location_id")
      .agg(count("*") as "frequency")
      //.filter($"pu_location_id" =!= $"do_location_id")
      .filter($"pu_location_id".isin(conLocList: _*) && $"do_location_id".isin(conLocList: _*))
      .filter(!$"pu_location_id".isin(isoLocList: _*) && !$"do_location_id".isin(isoLocList: _*))
      .as[TaxiTripFreq]
  }

  def profileTaxiTrips(spark: SparkSession, tripFreqDS: Dataset[TaxiTripFreq], path: String): Unit = {
    import spark.implicits._

    val tripFreqDS1 = tripFreqDS.filter($"pu_location_id" === $"do_location_id") // trips to and from the same location
    tripFreqDS1.groupBy("pu_location_id", "do_location_id")
      .agg(sum("frequency") as "frequency")
      .coalesce(1)
      .orderBy(desc("frequency"))
      .withColumn("percentage", col("frequency") * 100 / tripFreqDS1.agg(sum("frequency")).head.getAs[Long](0))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .csv(f"$path/$borough/single_location_trip_count_dist")

    val tripFreqDS2 = tripFreqDS.filter($"pu_location_id" =!= $"do_location_id") // trips to and from different locations
    tripFreqDS2.groupBy("pu_location_id")
      .agg(sum("frequency") as "frequency")
      .coalesce(1)
      .orderBy(desc("frequency"))
      .withColumn("percentage", col("frequency") * 100 / tripFreqDS2.agg(sum("frequency")).head.getAs[Long](0))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .csv(f"$path/$borough/cross_location_from_trip_count_dist")
    tripFreqDS2.groupBy("do_location_id")
      .agg(sum("frequency") as "frequency")
      .coalesce(1)
      .orderBy(desc("frequency"))
      .withColumn("percentage", col("frequency") * 100 / tripFreqDS2.agg(sum("frequency")).head.getAs[Long](0))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .csv(f"$path/$borough/cross_location_to_trip_count_dist")
    // The top 8 to and from locations 100% overlap with each other
  }

  // step-3
  def getTripPathFreqDS(spark: SparkSession, tripFreqDS: Dataset[TaxiTripFreq],
                        graphBroadcast: Broadcast[WeightedGraph[Long]]): Dataset[TripPathFreq] = {
    import spark.implicits._

    tripFreqDS.map(tf => {
      val result = Dijkstra.findShortestPaths(tf.pu_location_id, graphBroadcast.value)
      val path = Dijkstra.findPath(tf.do_location_id, result.parents)
      TripPathFreq(tf.frequency, path.mkString(","))
    })
  }

  def savePathFreqOutput(tripPathFreqDS: Dataset[TripPathFreq], path: String): Unit = {
    tripPathFreqDS.coalesce(1)
      .orderBy(desc("frequency"))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .csv(f"$path")
    //.csv(f"$path/$borough") // TODO
  }

  def savePathCoverageOutput(spark: SparkSession, tripPathCoverageDS: Dataset[TripPathCoverageCount], path: String): Unit = {
    import spark.implicits._

    val toSaveDS = tripPathCoverageDS.select("coverage_count", "trip_path").as[TripPathCoverageCount]
    toSaveDS.coalesce(1)
      .orderBy(desc("coverage_count"))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .csv(f"$path")
  }

  def saveHumanReadablePathCoverageOutput(spark: SparkSession, hrTripPathCoverageDS: Dataset[HumanReadableTripPathCoverageCount], path: String): Unit = {
    import spark.implicits._

    val toSaveDS = hrTripPathCoverageDS.select("coverage_count", "route").as[HumanReadableTripPathCoverageCount]
    toSaveDS.coalesce(1)
      .orderBy(desc("coverage_count"))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .csv(f"$path")
  }

  def saveTopKHumanReadableRouteCoveragePercentageOutput(spark: SparkSession,
                                                         topKHumanReadableRouteCoveragePercentageDS: Dataset[TopKHumanReadableRouteCoveragePercentage],
                                                         path: String): Unit = {
    import spark.implicits._

    val toSaveDS = topKHumanReadableRouteCoveragePercentageDS.select("coverage_percentage", "route").as[TopKHumanReadableRouteCoveragePercentage]
    toSaveDS.coalesce(1)
      .orderBy(desc("coverage_percentage"))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .csv(f"$path")
  }


  // step-4 util
  def addIntToValueArray(key: String, valueToAdd: Int, myMap: mutable.Map[String, Array[Int]]): Unit = {
    val currentValueArray = myMap.getOrElse(key, Array.empty[Int])
    val updatedValueArray = currentValueArray :+ valueToAdd
    myMap(key) = updatedValueArray
  }

  // step-4 util
  def addStringToValueArray(key: String, valueToAdd: String, myMap: mutable.Map[String, Array[String]]): Unit = {
    val currentValueArray = myMap.getOrElse(key, Array.empty[String])
    val updatedValueArray = currentValueArray :+ valueToAdd
    myMap(key) = updatedValueArray
  }

  // step-4
  def computeTripCoverageCount(spark: SparkSession, freqPathDF: DataFrame): (Dataset[TripPathCoverageCount], mutable.Map[String,Array[String]]) = {
    import spark.implicits._
    // Compute pairRDD key->path, value->frequency of trips for that path
    val pathFreqRDD = freqPathDF.rdd.map(row => (row.getString(1), row.getInt(0)))
    val pathFreqMap: Map[String, Int] = pathFreqRDD.collect().toMap
    val trips = pathFreqRDD.keys.collect()
    val n = trips.length
    val mutablePathCoverageCountMap: mutable.Map[String, Array[Int]] = mutable.Map.empty[String, Array[Int]]
    val mutablePathSubPathMap: mutable.Map[String, Array[String]] = mutable.Map.empty[String, Array[String]]
    pathFreqMap.foreach { case (key, value) => mutablePathCoverageCountMap(key) = Array(value) }
    pathFreqMap.foreach { case (key, _) => mutablePathSubPathMap(key) = Array(key) }
    // Update the parent of trip1 as trip2 if trip1 is a subsequence of trip2
    // here parent represents the bigger path which covers a given smaller path
    for (i <- 0 until n) {
      for (j <- 0 until n) {
        val trip1 = trips(i)
        val trip2 = trips(j)
        if (i != j) {
          if (isSubsequence(trip1, trip2)) {
            val key = trip1
            val valuetoAdd = pathFreqMap.getOrElse(key, 0)
            addIntToValueArray(trip2, valuetoAdd, mutablePathCoverageCountMap)
            addStringToValueArray(trip2, trip1, mutablePathSubPathMap)
          }
          // println(s"Trip1: $trip1 is a subsequence of Trip2: $trip2")
        }
      }
    }
    val pathCoverageCountRDD: RDD[(String, BigInt)] = spark.sparkContext.parallelize(
      mutablePathCoverageCountMap.toSeq.map { case (key, values) =>
        (key, BigInt(values.sum))
      }
    )
    val pathCoverageCountDS = pathCoverageCountRDD.map({
      case (trip_path, coverage_count) => TripPathCoverageCount(trip_path, coverage_count)
    }).toDS()

    (pathCoverageCountDS, mutablePathSubPathMap)
  }

  // step-5 util
  def topKTripsWithAtleastMStops(spark: SparkSession, k: Int, m: Int, df: DataFrame): Dataset[TripPathCoverageCount] = {
    import spark.implicits._

    val filteredDS: Dataset[TripPathCoverageCount] = df.as[TripPathCoverageCount].filter({ data => data.trip_path.split(",").length >= m })
    val sortedDS: Dataset[TripPathCoverageCount] = filteredDS.orderBy($"coverage_count".desc)
    sortedDS.limit(k).as[TripPathCoverageCount]
  }

  // step-5 util
  def mapLocationIds(input: String, locationIdZoneMap: Map[Int, String]): String = {
    input.split(",").map(num => locationIdZoneMap.getOrElse(num.toInt, "NA")).mkString(",")
  }

  // step-5
  def getTopKHumanReadbleRouteWithCoverage(spark: SparkSession, k: Int, m: Int, pathCoverageCountDF: DataFrame, mapLocationIdsUDF: UserDefinedFunction): DataFrame = {
    // get top-K routes with atleast m stops
    var topKDF = topKTripsWithAtleastMStops(spark, k, m, pathCoverageCountDF).toDF()
    topKDF.withColumn("route", mapLocationIdsUDF(col("trip_path")))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RecommendPublicTransportRoutes").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val options = parseMainOpts(Map(), args.toList)
    val nsDisInputPath = options(keyNeighborsDistanceInput).asInstanceOf[String]
    val tlcInputPath = options(keyTLCInput).asInstanceOf[String]
    val profileOutputPath = options(keyProfileOutput).asInstanceOf[String]
    val pathFreqOutputPath = options(keyPathFreqOutput).asInstanceOf[String]
    val pathCoverageOutputPath = options(keyPathCoverageOutput).asInstanceOf[String]
    val pathHRCoverageOutputPath = options(keyPathHRCoverageOutput).asInstanceOf[String]
    val pathHRCoveragePercentOutputPath = options(keyPathHRCoveragePercentOutput).asInstanceOf[String]

    // step-1: build up the neighbor zone graph
    val conLocList = getBoroughConnectedLocationList(borough)
    val isoLocList = getBoroughIsolatedLocationList(borough)
    val graphBroadcast = sc.broadcast(buildZoneNeighboringGraph(spark, nsDisInputPath))
    assert(graphBroadcast.value.nodes.size == conLocList.size)

    // step-2: compute taxi trip frequency
    val rawTrips = loadCleanData(spark, tlcInputPath)
    val tripFreqDS = getTaxiTripFreqDS(spark, rawTrips, conLocList, isoLocList).cache()
    profileTaxiTrips(spark, tripFreqDS, profileOutputPath)

    // step-3: transform each taxi trip in the frequency dataset into corresponding shortest path
    val pathFreqDS = getTripPathFreqDS(spark, tripFreqDS.filter($"pu_location_id" =!= $"do_location_id"), graphBroadcast)
    savePathFreqOutput(pathFreqDS, pathFreqOutputPath)
    assert(pathFreqDS.count() == loadRawDataCSV(spark, pathFreqOutputPath, delimiter = "\t").count())
    tripFreqDS.unpersist()

    // step-4: compute coverage count for each trip path
    val freqPathDF = loadRawDataCSV(spark, pathFreqOutputPath, delimiter = "\t")
    val (pathCoverageCountDS, mutablePathSubPathMap) = computeTripCoverageCount(spark, freqPathDF)
    savePathCoverageOutput(spark, pathCoverageCountDS, pathCoverageOutputPath)
    assert(pathCoverageCountDS.count() == loadRawDataCSV(spark, pathCoverageOutputPath, delimiter = "\t").count())

    // step-5: Recommend the top k trip paths of at least length m of the highest coverage count as human-readable routes
    val pathCoverageCountDF = loadRawDataCSV(spark, pathCoverageOutputPath, delimiter = "\t")
    val lookupPath = "/user/wl2484_nyu_edu/project/data/source/tlc/zone_lookup"
    val lookupDF = loadRawDataCSV(spark, lookupPath, delimiter = ",")
    val lookupMap: Map[Int, String] = lookupDF.withColumn("Zone1", split(col("Zone"), "/")(0)).select("LocationID", "Zone1").as[(Int, String)].rdd.collect().toMap
    val mapLocationIdsUDF = udf((input: String) => mapLocationIds(input, lookupMap))
    val k = 10000
    val m = 2
    val topKHumanReadableRouteDF = getTopKHumanReadbleRouteWithCoverage(spark, k, m, pathCoverageCountDF, mapLocationIdsUDF)
    val topKHumanReadableRouteDS = topKHumanReadableRouteDF.as[HumanReadableTripPathCoverageCount]
    saveHumanReadablePathCoverageOutput(spark, topKHumanReadableRouteDS, pathHRCoverageOutputPath)

    // TODO: step-6: Compute the coverage of taxi trips by the top k recommended routes
    val total_trips = freqPathDF.rdd.map(row => (row.getString(1), row.getInt(0))).map(row => row._2).reduce(_+_)
    val topKHumanReadableRouteCoveragePercentageDF = topKHumanReadableRouteDF.withColumn("coveragePercentage", round((col("coverage_count")/ total_trips)*100.0, 2))
    val topKHumanReadableRouteCoveragePercentageDS = topKHumanReadableRouteCoveragePercentageDF.as[TopKHumanReadableRouteCoveragePercentage]
    saveTopKHumanReadableRouteCoveragePercentageOutput(spark, topKHumanReadableRouteCoveragePercentageDS, pathHRCoveragePercentOutputPath)
  }
}
