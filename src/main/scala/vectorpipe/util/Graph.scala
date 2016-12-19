package vectorpipe.util

import spire.algebra._
import spire.std.any._
import spire.syntax.order._
import scalaz.State
import scalaz.syntax.applicative._

// --- //

/** Adapted from the Haskell implementation, which was based on the paper
  * 'Structuring Depth-First Search Algorithms in Haskell' by David King
  * and John Launchbury.
  *
  * Do not attempt to `new Graph(...)` yourself. In all cases,
  * use `Graph.fromEdges` to construct a Graph.
  */
class Graph[K: Order, V](
  outgoing: Vector[Seq[Vertex]],
  getNode: Vertex => (K, V, Seq[K]),
  getVert: K => Option[Vertex]
) {
  /** Possibly find a node that corresponds to a given key. */
  private def vertex(key: K): Option[Vertex] = getVert(key)

  /** Use [[get]]. Retrieve the full node information given a [[Vertex]].
    * Vertex validity (bounds) is not checked.
    */
  def node(v: Vertex): (K, V, Seq[K]) = getNode(v)

  /** Given a key value, retrieve the corresponding node value, if it exists. */
  def get(key: K): Option[V] = vertex(key).map(v => node(v)._2)

  /** The number of nodes in this Graph. */
  def size: Int = outgoing.length

  // --- ALGORITHM: Topological Sort --- //

  /** A topological sort of this Graph. */
  def topSort: Seq[Vertex] = dff.map(_.postorder).flatten.reverse

  // --- ALGORITHM: Spanning Forest --- //

  /** Some spanning Forest of this Graph. */
  def dff: Forest[Vertex] = spanningForest((0 until outgoing.length))

  /** Perform a depth-first search on this Graph based on some order
    * of vertices to produce a spanning forest of this Graph.
    *
    * Passing the result of `topSort` will yield a forest where each
    * [[Tree]] follows a topological hierarchy.
    */
  def spanningForest(vs: Seq[Vertex]): Forest[Vertex] = prune(vs.map(generate))

  /** Generate potentially infinite-depth [[Tree]]s for later pruning.
    * We use `toStream` here to avoid infinite recursion.
    */
  private def generate(v: Vertex): Tree[Vertex] = Tree(v, outgoing(v).toStream.map(generate))

  /** Prune unnecessary connections. */
  private def prune(vs: Forest[Vertex]): Forest[Vertex] = chop(vs).eval(Set.empty[Vertex])

  /** An alias to massage the `Applicative` use in [[chop]]. */
  type SetState[T] = State[Set[Vertex], T]

  private def chop(vs: Forest[Vertex]): SetState[Forest[Vertex]] = vs match {
    case Seq() => Seq.empty[Tree[Vertex]].point[SetState]
    case tree +: rest => State.get[Set[Vertex]].flatMap({
      case set if set.contains(tree.root) => chop(rest) /* We've been here already */
      case set => for {
        _  <- State.put(set + tree.root)
        as <- chop(tree.children)
        bs <- chop(rest)
      } yield {
        Tree(tree.root, as) +: bs
      }
    })
  }
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

    /* `flatMap` has the effect of ignoring target nodes which don't exist */
    val graph: Vector[Seq[Int]] =
      sorted.map({ case (_, _, ks) => ks.flatMap(k => getVert(k)) }).toVector

    new Graph(graph, { v: Vertex => vertMap(v) }, getVert)
  }
}
