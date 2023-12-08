package graph

case class WeightedEdge[Long](destination: Long, weight: Int)

class WeightedGraph[Long](adjList: Map[Long, List[WeightedEdge[Long]]]) extends Graph[Long] {
  override def nodes: List[Long] = adjList.keys.toList
  override def edges: List[(Long, Long)] = adjList.flatMap {
    case (node, edges) => edges.map(edge => (node, edge.destination))
  }.toList

  override def addNode(n: Long): WeightedGraph[Long] = new WeightedGraph(adjList + (n -> List()))

  private def addEdge(from: Long, weightedEdge: WeightedEdge[Long]): WeightedGraph[Long] = {
    val fromNeighbours = weightedEdge +: adjList.getOrElse(from, Nil)
    val g = new WeightedGraph(adjList + (from -> fromNeighbours))
    val to = weightedEdge.destination
    if (g.nodes.contains(to)) g else g.addNode(to)
  }

  override def addEdge(from: Long, to: Long): WeightedGraph[Long] = addEdge(from, WeightedEdge(to, 0))

  override def neighbours(node: Long): List[Long] = adjList.getOrElse(node, Nil).map(_.destination)

  def neighboursWithWeights(node: Long): List[WeightedEdge[Long]] = adjList.getOrElse(node, Nil)
}
