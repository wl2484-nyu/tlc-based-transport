import etl.TaxiZoneNeighboring.{getBoroughConnectedLocationMap, getBoroughIsolatedLocationList, loadLocationNeighborsDistanceByBorough}
import etl.Utils.{keyNeighborsDistanceInput, parseMainOpts}
import graph.{WeightedEdge, WeightedGraph}
import org.apache.spark.sql.SparkSession

object Main {
  val borough = "Manhattan" // target borough

  // step-1
  def buildZoneNeighboringGraph(spark: SparkSession, neighborsDistanceInputPath: String): WeightedGraph[Long] = {
    import spark.implicits._

    val zoneNeighborDisDS = loadLocationNeighborsDistanceByBorough(spark, neighborsDistanceInputPath, borough=borough)
    new WeightedGraph[Long](zoneNeighborDisDS.map(r =>
      (r.location_id, WeightedEdge(r.neighbor_location_id, r.distance))).rdd
      .groupByKey()
      .mapValues(_.toList)
      .collect
      .toMap)
  }

  // step-2
  def createRDDFrequency(spark: SparkSession, intermediatePath: String): RDD[Row] = {
    import spark.implicits._
    val years = Seq(2020, 2021, 2022)
    val cabs = Seq("fhv","fhvhv","yellow","green")
    val schema = new StructType().add(StructField("PUDOPair", new StructType().add(StructField("pulocationID", LongType, true)).add(StructField("dolocationID", LongType, true)), true)).add(StructField("Frequency", LongType, true))
    var resultDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val unionedDFs = for {
      cab <- cabs
      year <- years
    } yield loadintermediateData(spark, intermediatePath, year, cab)
    resultDF = unionedDFs.reduce((df1, df2) => df1.union(df2))
    resultDF.rdd
  }

  // step-3
  def shortestPath(spark: SparkSession, frequencyRDD: Array[Row], graphBroadcast: Broadcast[WeightedGraph[Long]]): Array[(Long, Long, Long, List[Long])] = {
    frequencyRDD.map { row =>
      val source = row.getAs[Row](0).getLong(0)
      val destination = row.getAs[Row](0).getLong(1)
      val frequency = row.getLong(1)

      val result = Dijkstra.findShortestPaths(source, graphBroadcast.value)

      // Return a tuple containing source, destination, frequency, and shortest path
      (source, destination, frequency, Dijkstra.findPath(destination, result.parents))
    }
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
    val frequencyData = createRDDFrequency(spark, intermediatePath).collect()
    
    // TODO: step-3: transform each taxi trip in the frequency RDD into corresponding shortest path
    val shortestPath = shortestPath(spark,frequencyData,graphBroadcast)
    
    // TODO: step-4: compute coverage count for each trip path
    step4()

    // TODO: step-5: Recommend the top k trip paths of at least length m of the highest coverage count as human-readable routes
    step5()

    // TODO: step-6: Compute the coverage of taxi trips by the top k recommended routes
    step6()

  }
}
