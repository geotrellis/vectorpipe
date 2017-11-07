package vectorpipe.osm.internal

import java.time.Instant

import scala.annotation.tailrec

import cats.implicits._
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import vectorpipe.osm._

// --- //

private[vectorpipe] object PlanetHistory {

  // TODO Temporary?
  def lines(nodes: RDD[(Long, Node)], ways: RDD[(Long, Way)]): RDD[OSMLine] =
    joinedWays(nodes, ways).flatMap { case (id, (ws, ns)) => linesAndPolys(ws.toList, ns.toList)._1 }

  /** Given one RDD of nodes and one of ways, produce an RDD of all nodes/ways keyed by the WayID
    * to which they are all related.
    */
  private[this] def joinedWays(
    nodes: RDD[(Long, Node)],
    ways: RDD[(Long, Way)]
  ): RDD[(Long, (Iterable[Way], Iterable[Node]))] = {

    val nodesToWayIds: RDD[(Node, Iterable[Long])] =
      nodes
        .cogroup(ways.flatMap { case (wayId, way) => way.nodes.map { nodeId => (nodeId, wayId) } })
        .flatMap {
          /* ASSUMPTION: `nodes` contains distinct elements */
          case (_, (nodes, wayIds)) => nodes.headOption.map(n => (n, wayIds))
        }

    val wayIdToNodes: RDD[(Long, Node)] =
      nodesToWayIds.flatMap { case (node, wayIds) => wayIds.map(wayId => (wayId, node)) }

    ways.cogroup(wayIdToNodes)
  }

  /** Given likely unordered collections of Ways and their associated Nodes,
    * construct GeoTrellis Lines and Polygons such that:
    *   - there is a Line present for every updated Way
    *   - there is a Line present for every set of updated Nodes (i.e. those with matching timestamps)
    *   - there is a Polygon present instead of a Line from the above two conditions when the Way is "closed"
    */
  private[vectorpipe] def linesAndPolys(ways: List[Way], nodes: List[Node]): (List[OSMLine], List[OSMPolygon]) = {

    @tailrec def work(
      ws: List[Way],
      ls: List[OSMLine],
      ps: List[OSMPolygon]
    ): (List[OSMLine], List[OSMPolygon]) = ws match {
      case Nil => (ls, ps)
      case w :: rest => {

        /* Y2K bug here, except it won't manifest until the end of the year 1 billion AD */
        val nextTime: Instant = rest.headOption.map(_.data.meta.timestamp).getOrElse(Instant.MAX)

        /* Each full set of Nodes that would have existed for each time slice. */
        val allSlices: List[(Instant, Map[Long, Node])] =
          changedNodes(w.data.meta.timestamp, nextTime, nodes)
            .groupBy(_.data.meta.timestamp)
            .toList
            .sortBy { case (i, _) => i }
            .scanLeft((w.data.meta.timestamp, recentNodes(w, nodes))) { case ((_, p), (i, changes)) =>

              val replaced: Map[Long, Node] = changes.foldLeft(p) {
                /* The Node was deleted from this Way */
                case (acc, node) if !node.data.meta.visible => acc - node.data.meta.id
                /* The Node was moved or had its metadata updated */
                case (acc, node) => acc.updated(node.data.meta.id, node)
              }

              (i, replaced)
            }

        if (w.isClosed) {
          val everyPoly: List[OSMPolygon] = feature({ ps => Polygon(Line(ps)) }, w, allSlices)

          work(rest, ls, ps ++ everyPoly)
        } else {
          val everyLine: List[OSMLine] = feature({ ps => Line(ps) }, w, allSlices)

          work(rest, ls ++ everyLine, ps)
        }

      }
    }

    /* Ensure the Ways are sorted before proceeding */
    work(ways.sortBy(_.data.meta.timestamp), Nil, Nil)
  }

  /** Construct GeoTrellis Features from the given time slices. */
  private[this] def feature[G <: Geometry](
    f: Vector[Point] => G,
    w: Way,
    slices: List[(Instant, Map[Long, Node])]
  ): List[Feature[G, ElementData]] = {
    slices.foldLeft(Nil: List[Feature[G, ElementData]]) { case (acc, (i, ns)) =>
      /* If a Node were deleted that the Way still expected, then no Line
       * should be formed.
       */
      val points: Option[Vector[Point]] =
        w.nodes.map(id => ns.get(id).map(n => Point(n.lon, n.lat))).sequence

      points.map { ps =>
        // TODO Overwrite more values?
        // TODO Lenses!
        val feature = Feature(f(ps), w.data.copy(meta = w.data.meta.copy(timestamp = i)))
        feature :: acc
      }.getOrElse(acc)
    }
  }

  /** Given a collection of all Nodes ever associated with a [[Way]], which subset
    * of them are the "most recent" from the perspective of the given Way?
    */
  private[this] def recentNodes(way: Way, nodes: List[Node]): Map[Long, Node] = {
    nodes
      .filter(!_.data.meta.timestamp.isAfter(way.data.meta.timestamp))
      .groupBy(_.data.meta.id)
      .map { case (k, ns) => (k, ns.maxBy(_.data.meta.timestamp)) }
  }

  /** Find all the Nodes that were created/changed between two timestamps. */
  private[this] def changedNodes(t0: Instant, t1: Instant, nodes: List[Node]): List[Node] =
    nodes.filter(n => n.data.meta.timestamp.isAfter(t0) && n.data.meta.timestamp.isBefore(t1))

}
