package graph
trait Graph[Long] {
  def nodes: List[Long]
  def edges: List[(Long, Long)]
  def addNode(n: Long): Graph[Long]
  def addEdge(from: Long, to: Long): Graph[Long]
  def neighbours(node: Long): List[Long]
}

object Graph {
  def apply[Long](adjList: Map[Long, List[Long]]): Graph[Long] = new DirectedGraph(adjList)
  def apply[Long](): Graph[Long] = new DirectedGraph(Map[Long, List[Long]]())
}
