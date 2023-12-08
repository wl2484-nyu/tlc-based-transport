package graph

object Dijkstra {
  case class ShortStep[N](parents: Map[N, N], unprocessed: Set[N], distances: Map[N, Int]) {
    def extractMin(): Option[(N, Int)] = Try(unprocessed.minBy(n => distances(n))).toOption.map(n => (n, distances(n)))
  }

  def findShortestPaths[N](source: N, graph: WeightedGraph[N]): ShortStep[N] = {
    val sDistances: Map[N, Int] = graph.nodes.map(_ -> Int.MaxValue).toMap + (source -> 0)

    def shortestPath(step: ShortStep[N]): ShortStep[N] = {
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

  private def findPathRec[N](node: N, parents: Map[N, N]): List[N] =
    parents.get(node).map(parent => node +: findPathRec(parent, parents)).getOrElse(List(node))

  def findPath[N](destination: N, parents: Map[N, N]): List[N] = {
    findPathRec(destination, parents).reverse
  }
}
