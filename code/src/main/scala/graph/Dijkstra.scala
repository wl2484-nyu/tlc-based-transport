package Graph

import graph.WeightedGraph
import graph.WeightedEdge
import scala.util.Try

object Dijkstra {
  case class ShortStep[Long](parents: Map[Long, Long], unprocessed: Set[Long], distances: Map[Long, Int]) {
    def extractMin(): Option[(Long, Int)] = Try(unprocessed.minBy(n => distances(n))).toOption.map(n => (n, distances(n)))
  }

  def findShortestPaths[Long](source: Long, graph: WeightedGraph[Long]): ShortStep[Long] = {
    val sDistances: Map[Long, Int] = graph.nodes.map(_ -> Int.MaxValue).toMap + (source -> 0)

    def shortestPath(step: ShortStep[Long]): ShortStep[Long] = {
      step.extractMin().map {
        case (node, currentDistance) =>
          val newDist = graph.neighboursWithWeights(node).collect {
            case WeightedEdge(neighbour, neighbourDistance) if step.distances.get(neighbour).exists(_ > currentDistance + neighbourDistance) =>
              neighbour -> (currentDistance + neighbourDistance)
          }

          val newParents = newDist.map { case (neighbour, _) => neighbour -> node }

          shortestPath(ShortStep(
            step.parents ++ newParents,
            step.unprocessed - node,
            step.distances ++ newDist))
      }.getOrElse(step)
    }

    shortestPath(ShortStep(Map(), graph.nodes.toSet, sDistances))
  }

  private def findPathRec[Long](node: Long, parents: Map[Long, Long]): List[Long] =
    parents.get(node).map(parent => node +: findPathRec(parent, parents)).getOrElse(List(node))

  def findPath[Long](destination: Long, parents: Map[Long, Long]): List[Long] = {
    findPathRec(destination, parents).reverse
  }
}
