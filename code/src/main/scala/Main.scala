import etl.TaxiZoneNeighboring.{getBoroughConnectedLocationMap, getBoroughIsolatedLocationList, loadLocationNeighborsDistanceByBorough}
import etl.Utils.{keyNeighborsDistanceInput, parseMainOpts}
import graph.{Dijkstra, WeightedEdge, WeightedGraph}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {
  val borough = "Manhattan" // target borough

  case class TripFreq(pu_location_id: Long, do_location_id: Long, frequency: BigInt)

  case class ShortestPathFreq(frequency: BigInt, path: String)

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

  // step-3
  def shortestPath(spark: SparkSession, frequencyDS: Dataset[TripFreq], graphBroadcast: Broadcast[WeightedGraph[Long]]): Dataset[ShortestPathFreq] = {
    import spark.implicits._

    frequencyDS.map(tf => {
      val result = Dijkstra.findShortestPaths(tf.pu_location_id, graphBroadcast.value)
      val path = Dijkstra.findPath(tf.do_location_id, result.parents)
      ShortestPathFreq(tf.frequency, path.mkString(","))
    })
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

    val options = parseMainOpts(Map(), args.toList)
    val nsDisInputPath = options(keyNeighborsDistanceInput).asInstanceOf[String]
    val intermediatePath = "/user/wl2484_nyu_edu/project/data/intermediateFrequencyData/tlc/cabs"

    // step-1: build up the neighbor zone graph
    val conLocMapBroadcast = sc.broadcast(getBoroughConnectedLocationMap(borough))
    val isoLocListBroadcast = sc.broadcast(getBoroughIsolatedLocationList(borough))
    val graphBroadcast = sc.broadcast(buildZoneNeighboringGraph(spark, nsDisInputPath))
    assert(graphBroadcast.value.nodes.size == conLocMapBroadcast.value.keys.size)

    // TODO: step-2: compute taxi trip frequency

    // TODO: step-3: transform each taxi trip in the frequency RDD into corresponding shortest path

    // TODO: step-4: compute coverage count for each trip path
    step4()

    // TODO: step-5: Recommend the top k trip paths of at least length m of the highest coverage count as human-readable routes
    step5()

    // TODO: step-6: Compute the coverage of taxi trips by the top k recommended routes
    step6()

  }
}
