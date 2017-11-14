package vectorpipe.osm.internal

import java.time.Instant

import scala.annotation.tailrec

import cats.implicits._
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import vectorpipe.osm._

// --- //

private[vectorpipe] object PlanetHistory {

  /** For all given Ways, associate with their Nodes to produce GeoTrellis Lines and Polygons. */
  def features(nodes: RDD[(Long, Node)], ways: RDD[(Long, Way)]): (RDD[OSMLine], RDD[OSMPolygon]) = {

    val lap: RDD[(List[OSMLine], List[OSMPolygon])] = joinedWays(nodes, ways).map { case (_, (ws, ns)) =>
      linesAndPolys(ws.toList, ns.toList)
    }

    /* Unfortunate but necessary, since there is no `RDD.unzip` and no `RDD.flatten`. */
    (lap.keys.flatMap(identity), lap.values.flatMap(identity))
  }

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
      /* OSM Extracts can produce degenerate Ways. These two checks guard against those. */
      case w :: _ if w.nodes.length < 4 && w.isClosed => (ls, ps)
      case w :: _ if w.nodes.length < 2 => (ls, ps)
      case w :: rest => {

        /* Y2K bug here, except it won't manifest until the end of the year 1 billion AD */
        val nextTime: Instant = rest.headOption.map(_.meta.timestamp).getOrElse(Instant.MAX)

        /* Each full set of Nodes that would have existed for each time slice. */
        val allSlices: List[(ElementMeta, Map[Long, Node])] =
          changedNodes(w.meta.timestamp, nextTime, nodes)
            .groupBy(_.meta.timestamp)
            .toList
            .sortBy { case (i, _) => i }
            .scanLeft((w.meta, recentNodes(w, nodes))) { case ((_, p), (_, changes)) =>

              /* All Nodes changed during this timeslice are assumed to have been
               * changed by the same user. Anything else would be highly unlikely.
               * Therefore, we can ask for the metadata of the first changed Node
               * we find (the `.head`).
               */
              val meta: ElementMeta = changes.head.meta

              /* The user who changed the Nodes in this timeslice is credited
               * with the creation of the `Line`, even if they weren't the one
               * who created the Way to begin with.
               */
              val credited: ElementMeta = w.meta.copy(
                user      = meta.user,
                uid       = meta.uid,
                changeset = meta.changeset,
                timestamp = meta.timestamp
              )

              val replaced: Map[Long, Node] = changes.foldLeft(p) {
                /* The Node was deleted from this Way */
                case (acc, node) if !node.meta.visible => acc - node.meta.id
                /* The Node was moved or had its metadata updated */
                case (acc, node) => acc.updated(node.meta.id, node)
              }

              (credited, replaced)
            }

        w match {
          case _ if w.isLine => work(rest, ls ++ feature({ ps => Line(ps) }, w, allSlices), ps)
          case _ => work(rest, ls, ps ++ feature({ ps => Polygon(Line(ps)) }, w, allSlices))
        }
      }
    }

    /* Ensure the Ways are sorted before proceeding */
    work(ways.sortBy(_.meta.timestamp), Nil, Nil)
  }

  /** Construct GeoTrellis Features from the given time slices. */
  private[this] def feature[G <: Geometry](
    f: Vector[Point] => G,
    w: Way,
    slices: List[(ElementMeta, Map[Long, Node])]
  ): List[Feature[G, ElementMeta]] = {
    slices.foldLeft(Nil: List[Feature[G, ElementMeta]]) { case (acc, (m, ns)) =>
      /* If a Node were deleted that the Way still expected, then no Line
       * should be formed.
       */
      val points: Option[Vector[Point]] =
        w.nodes.map(id => ns.get(id).map(n => Point(n.lon, n.lat))).sequence

      points.map { ps => Feature(f(ps), m) :: acc }.getOrElse(acc)
    }
  }

  /** Given a collection of all Nodes ever associated with a [[Way]], which subset
    * of them are the "most recent" from the perspective of the given Way?
    */
  private[this] def recentNodes(way: Way, nodes: List[Node]): Map[Long, Node] = {
    nodes
      .filter(!_.meta.timestamp.isAfter(way.meta.timestamp))
      .groupBy(_.meta.id)
      .map { case (k, ns) => (k, ns.maxBy(_.meta.timestamp)) }
  }

  /** Find all the Nodes that were created/changed between two timestamps. */
  private[this] def changedNodes(t0: Instant, t1: Instant, nodes: List[Node]): List[Node] =
    nodes.filter(n => n.meta.timestamp.isAfter(t0) && n.meta.timestamp.isBefore(t1))

}
