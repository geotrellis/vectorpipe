package vectorpipe.util

// --- //

// Is it even an adjacency matrix?
// TODO Can this be newtyped (AnyVal'd)?
case class Graph[K, V](table: Vector[(K, V)]) {
  def topSort: Graph[K, V] = ???
}

object Graph {
  def fromEdges[K, V](edges: Seq[(K, V, Seq[K])]): Graph[K, V] = ???
}
