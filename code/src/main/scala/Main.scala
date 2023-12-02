import org.apache.spark.sql.SparkSession

object Main {

  // step-1
  def step1(): Unit = {}

  // step-2
  def step2(): Unit = {}

  // step-3
  def step3(): Unit = {}

  // step-4
  def step4(): Unit = {}

  // step-5
  def step5(): Unit = {}

  // step-6
  def step6(): Unit = {}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RecommendPublicTransportRoutes").getOrCreate()

    // TODO: step-1: build up the neighbor zone graph
    step1()

    // TODO: step-2: compute taxi trip frequency
    step2()

    // TODO: step-3: transform each taxi trip in the frequency RDD into corresponding shortest path
    step3()

    // TODO: step-4: compute coverage count for each trip path
    step4()

    // TODO: step-5: Recommend the top k trip paths of at least length m of the highest coverage count as human-readable routes
    step5()

    // TODO: step-6: Compute the coverage of taxi trips by the top k recommended routes
    step6()

  }
}