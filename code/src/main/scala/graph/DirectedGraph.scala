package graph

class DirectedGraph[Long](adjList: Map[Long, List[Long]]) extends Graph[Long] {
  override def nodes: List[Long] = adjList.keys.toList //returning the list of keys from the (getconnectedlocationmap)

  override def edges: List[(Long, Long)] = adjList.flatMap {
    case (node, neighbours) => neighbours.map(neighbour => (node, neighbour))
  }.toList  // creating the list of nodes from the adjacency list (4,244) ,(4,232), (4,79)

  override def addNode(n: Long): Graph[Long] = new DirectedGraph(adjList + (n -> List()))

  override def addEdge(from: Long, to: Long): Graph[Long] = {
    val fromNeighbours = to +: neighbours(from)
    val g = new DirectedGraph(adjList + (from -> fromNeighbours))
    if (g.nodes.contains(to)) g else g.addNode(to)
  }

  override def neighbours(node: Long): List[Long] = adjList.getOrElse(node, Nil)
}
