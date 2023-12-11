import etl.TLC.{TaxiTrip, loadCleanData}
import etl.TaxiZoneNeighboring.{getBoroughConnectedLocationList, getBoroughIsolatedLocationList, loadLocationNeighborsDistanceByBorough}
import etl.Utils._
import graph.{Dijkstra, WeightedEdge, WeightedGraph}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, count, desc, sum}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object Main {
  val borough = "Manhattan" // target borough

  case class TaxiTripFreq(pu_location_id: Long, do_location_id: Long, frequency: BigInt)

  case class TripPathFreq(frequency: BigInt, trip_path: String)

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

  // step-4
  def step4(): Unit = {}

  // step-5
  def step5(): Unit = {}

  // step-6
  def step6(): Unit = {}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RecommendPublicTransportRoutes").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val options = parseMainOpts(Map(), args.toList)
    val nsDisInputPath = options(keyNeighborsDistanceInput).asInstanceOf[String]
    val tlcInputPath = options(keyTLCInput).asInstanceOf[String]
    val profileOutputPath = options(keyProfileOutput).asInstanceOf[String]
    val pathFreqOutputPath = options(keyPathFreqOutput).asInstanceOf[String]

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

    // TODO: step-4: compute coverage count for each trip path
    step4()

    // TODO: step-5: Recommend the top k trip paths of at least length m of the highest coverage count as human-readable routes
    step5()

    // TODO: step-6: Compute the coverage of taxi trips by the top k recommended routes
    step6()

  }
}
