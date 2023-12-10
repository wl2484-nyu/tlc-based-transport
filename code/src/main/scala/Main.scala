import etl.TLC.{TaxiTrip, loadCleanData}
import etl.TaxiZoneNeighboring.{getBoroughConnectedLocationList, getBoroughIsolatedLocationList, loadLocationNeighborsDistanceByBorough}
import etl.Utils._
import graph.{Dijkstra, WeightedEdge, WeightedGraph}
import union_find.UnionFind
import scala.collection.Map
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{count, desc}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Main {
  val borough = "Manhattan" // target borough

  case class TaxiTripFreq(pu_location_id: Long, do_location_id: Long, frequency: BigInt)

  case class TripPathFreq(frequency: BigInt, trip_path: String)

  case class TripPathCoverageCount(trip_path: String, coverage_count: BigInt)

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

  def getTaxiTripFreqDS(spark: SparkSession, rawTrips: Dataset[TaxiTrip], conLocList: List[Long],
                        isoLocList: List[Long]): Dataset[TaxiTripFreq] = {
    import spark.implicits._

    rawTrips.select("pu_location_id", "do_location_id")
      .groupBy("pu_location_id", "do_location_id")
      .agg(count("*") as "frequency")
      .filter($"pu_location_id" =!= $"do_location_id")
      .filter($"pu_location_id".isin(conLocList: _*) && $"do_location_id".isin(conLocList: _*))
      .filter(!$"pu_location_id".isin(isoLocList: _*) && !$"do_location_id".isin(isoLocList: _*))
      .as[TaxiTripFreq]
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
  }

  def savePathCoverageOutput(tripPathCoverageDS: Dataset[TripPathCoverageCount], path: String): Unit = {
    val toSaveDS = tripPathCoverageDS.select("coverage_count", "trip_path").as[TripPathCoverageCount]
    toSaveDS.coalesce(1)
      .orderBy(desc("coverage_count"))
      .write
      .mode(SaveMode.Overwrite) // workaround for abnormal path-already-exists error
      .option("header", true)
      .option("delimiter", "\t")
      .csv(f"$path")
  }

  // step-4 util
  def topKTripsWithAtleastMStops(k: Int, m: Int, ds: Dataset[TripPathCoverageCount]): Dataset[TripPathCoverageCount] = {
    val filteredDS: Dataset[TripPathCoverageCount] = ds.filter({ data => data.trip_path.split(",").length >= m })
    val sortedDS: Dataset[TripPathCoverageCount] = filteredDS.orderBy($"coverage_count".desc)
    sortedDS.limit(k).as[TripPathCoverageCount]
  }


  // step-4
  def computeTripCoverageCount(spark: SparkSession, freqPathDF: DataFrame, ufString: UnionFind[String]):  Dataset[TripPathCoverageCount] = {
    // Compute pairRDD key->path, value->frequency of trips for that path
    val pathFreqRDD = freqPathDF.rdd.map(row => (row.getString(1), row.getInt(0)))
    val trips = pathFreqRDD.keys.collect()
    val n = trips.length

    // Update the parent of trip1 as trip2 if trip1 is a subsequence of trip2
    // here parent represents the bigger path which covers a given smaller path
    for (i <- 0 until n) {
      for (j <- i + 1 until n) {
        val trip1 = trips(i)
        val trip2 = trips(j)
        if (isSubsequence(trip1, trip2)) {
          // println("Trip1:", trip1, "is a subsequence of Trip2:", trip2)
          ufString.union(trip2, trip1)
        }
      }
    }

    // generate pairRDD (key->path, value->parent trip covering that path)
    var rowsOutput = Seq.empty[(String, String)]
    for (trip <- trips) {
      rowsOutput = rowsOutput :+ (trip -> ufString.find(trip))
    }
    val pathParentRDD = spark.sparkContext.parallelize(rowsOutput)

    assert(pathParentRDD.count() == pathFreqRDD.count())

    // make trip coverage count rdd

    // Left join RDD1 and RDD2
    val joinedRDD = pathFreqRDD.leftOuterJoin(pathParentRDD)
    // Perform left outer join and sum up the values
    val pathCoverageCountRDD = joinedRDD
      .map { case (key, (value, opt)) => (opt.getOrElse("invalidParent"), value) } // Extract key and value
      .reduceByKey(_ + _) // Sum up the trips that each parent covers

    pathCoverageCountRDD.map({
      case (trip_path, coverage_count) => TripPathCoverageCount(trip_path, coverage_count)
    }).toDS()

  }

  // step-5 util
  def mapLocationIds(input: String, idMap: Map[Integer, String]): String = {
    val inputValues = input.split(",").map(_.trim)
    val mappedValues = inputValues.flatMap { inputValue =>
      idMap.get(Integer.valueOf(inputValue))
    }

    if (mappedValues.nonEmpty) {
      mappedValues.mkString(", ")
    } else {
      s"No mapping found for input: $input"
    }
  }

  val mapLocationIdsUDF: Any = udf(mapLocationIds)

  // step-5
  def getTopKHumanReadbleRouteWithCoverage(spark: SparkSession, k: Int, m: Int, pathCoverageCountDF: DataFrame, lookupMap: Map[Int, String]) = {
    // get top-K routes with atleast m stops
    var topKDF = topKTripsWithAtleastMStops(10, 3, pathCoverageCountDS).toDF()
    topKDF.withColumn("route", mapLocationIdsUDF(col("trip_path"), mapLocationIdsUDF))

  }

  // step-6
  def step6(): Unit = {}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RecommendPublicTransportRoutes").getOrCreate()
    val sc = spark.sparkContext

    val options = parseMainOpts(Map(), args.toList)
    val nsDisInputPath = options(keyNeighborsDistanceInput).asInstanceOf[String]
    val tlcInputPath = options(keyTLCInput).asInstanceOf[String]
    val pathFreqOutputPath = options(keyPathFreqOutput).asInstanceOf[String]
    val pathCoverageOutputPath = options(keyPathCoverageOutput).asInstanceOf[String]

    val ufString = new UnionFind[String]

    // step-1: build up the neighbor zone graph
    val conLocList = getBoroughConnectedLocationList(borough)
    val isoLocList = getBoroughIsolatedLocationList(borough)
    val graphBroadcast = sc.broadcast(buildZoneNeighboringGraph(spark, nsDisInputPath))
    assert(graphBroadcast.value.nodes.size == conLocList.size)

    // step-2: compute taxi trip frequency
    val rawTrips = loadCleanData(spark, tlcInputPath)
    val tripFreqDS = getTaxiTripFreqDS(spark, rawTrips, conLocList, isoLocList)

    // step-3: transform each taxi trip in the frequency dataset into corresponding shortest path
    val pathFreqDS = getTripPathFreqDS(spark, tripFreqDS, graphBroadcast)
    savePathFreqOutput(pathFreqDS, pathFreqOutputPath)
    assert(pathFreqDS.count() == loadRawDataCSV(spark, pathFreqOutputPath, delimiter = "\t").count())

    // step-4: compute coverage count for each trip path
    val freqPathDF = loadRawDataCSV(spark, pathFreqOutputPath, delimiter = "\t")
    val pathCoverageCountDS = computeTripCoverageCount(spark, freqPathDF, ufString)
    savePathCoverageOutput(pathCoverageCountDS, pathCoverageOutputPath)
    assert(pathCoverageCountDS.count() == loadRawDataCSV(spark, pathCoverageOutputPath, delimiter = "\t").count())

    // TODO: step-5: Recommend the top k trip paths of at least length m of the highest coverage count as human-readable routes
    val pathCoverageCountDF = loadRawDataCSV(spark, pathCoverageOutputPath, delimiter = "\t")
    val lookupPath = "/user/wl2484_nyu_edu/project/data/source/tlc/zone_lookup"
    val lookupDF = loadRawDataCSV(spark, lookupPath, delimiter = ",")
    val lookupMap: Map[Int, String] = lookupDF.withColumn("Zone1", split(col("Zone"), "/")(0)).select("LocationID", "Zone1").as[(Int, String)].rdd.collectAsMap()
    val k = 10
    val m = 3
    val topKHumanReadableRouteDS = getTopKHumanReadbleRouteWithCoverage(spark, k, m, pathCoverageCountDF, lookupMap)

    // TODO: step-6: Compute the coverage of taxi trips by the top k recommended routes
    step6()

  }
}
