package vectorpipe.util

import spire.algebra._
import spire.std.any._
import spire.syntax.order._

// --- //

/** Adapted from the Haskell implementation, which was based on the paper
  * 'Structuring Depth-First Search Algorithms in Haskell' by David King
  * and John Launchbury.
  *
  * Do not attempt to `new Graph(...)` yourself. In all cases,
  * use `Graph.fromEdges` to construct a Graph.
  */
class Graph[K: Order, V](
  graph: Vector[Seq[Vertex]],
  getNode: Vertex => (K, V, Seq[K]),
  getVert: K => Option[Vertex]
) {
  /** Possibly find a node that corresponds to a given key. */
  def vertex(key: K): Option[Vertex] = getVert(key)

  /** Retrieve the full node information given a [[Vertex]].
    * Vertex validity (bounds) is not checked.
    */
  def node(v: Vertex): (K, V, Seq[K]) = getNode(v)

  /** Given a key value, retrieve the corresponding node value, if it exists. */
  def get(key: K): Option[V] = vertex(key).map(v => node(v)._2)

  /** A topological sort of this Graph. */
  def topSort: Seq[Vertex] = ???

  /** Perform a depth-first search on this Graph based on some order
    * of vertices to produce a spanning forest of this Graph.
    *
    * Passing the result of `topSort` will yield a forest where each
    * [[Tree]] follows a topological hierarchy.
    */
  def spanningForest(vs: Seq[Vertex]): Seq[Tree[Vertex]] = ???
}

object Graph {
  /** Construct a [[Graph]], given a list of key-value pairs that represent
    * a node, as well as their list of out-going edges.
    */
  def fromEdges[K: Order, V](edges: Seq[(K, V, Seq[K])]): Graph[K, V] = {
    val maxV: Int = edges.length - 1
    val sorted = edges.sortWith({ case ((k1, _, _), (k2, _, _)) => k1 < k2 })

    val keyMap: Vector[K] = sorted.map(_._1).toVector
    val vertMap: Vector[(K, V, Seq[K])] = sorted.toVector

    /* A cute little binary search */
    val getVert: K => Option[Vertex] = { k =>
      def work(a: Int, b: Int): Option[Vertex] = {
        if (a > b) return None  /* Early return */

        val mid = a + (b - a) / 2  // correct order of operations?

        k.compare(keyMap(mid)) match {
          case (-1) => work(a, mid - 1)
          case 0    => Some(mid)
          case 1    => work(mid + 1, b)
        }
      }

      work(0, maxV)
    }

    val graph = sorted.map({ case (_, _, ks) => ks.flatMap(k => getVert(k)) }).toVector

    new Graph(graph, { v: Vertex => vertMap(v) }, getVert)
  }
}
