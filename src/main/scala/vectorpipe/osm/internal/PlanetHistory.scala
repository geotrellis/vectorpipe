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
  def features(
    nodes: RDD[(Long, Node)],
    ways: RDD[(Long, Way)]
  ): (RDD[OSMPoint], RDD[OSMLine], RDD[OSMPolygon]) = {

    val (loneNodes, edges) = joinedWays(nodes, ways)

    val lap: RDD[(List[OSMLine], List[OSMPolygon])] = edges.map { case (_, (ws, ns)) =>
      linesAndPolys(ws.toList, ns.toList)
    }

    val points: RDD[OSMPoint]   = loneNodes.map { case (_, n) => Feature(Point(n.lat, n.lon), n.meta) }
    /* Unfortunate but necessary, since there is no `RDD.unzip` and no `RDD.flatten`. */
    val lines:  RDD[OSMLine]    = lap.keys.flatMap(identity)
    val polys:  RDD[OSMPolygon] = lap.values.flatMap(identity)

    (points, lines, polys)
  }

  /** Given one RDD of nodes and one of ways, produce:
    *   - an RDD of all nodes/ways keyed by the WayID to which they are all related
    *   - an RDD of all nodes keys of their NodeID which were never associated with any Way
    */
  private[this] def joinedWays(
    nodes: RDD[(Long, Node)],
    ways: RDD[(Long, Way)]
  ): (RDD[(Long, Node)], RDD[(Long, (Iterable[Way], Iterable[Node]))]) = {

    /* Forgive the `.distinct` here. If we don't do that, there will be Way ID duplication
     * in the first cogroup below.
     */
    val nodeIdsToWayIds: RDD[(Long, Long)] =
      ways.flatMap { case (wayId, way) => way.nodes.map { nodeId => (nodeId, wayId) }}
        .distinct

    /* Every version of every Node, paired with the IDs of Ways that need them AT ANY TIME. */
    val nodesToWayIds: RDD[(Node, Iterable[Long])] =
      nodes
        .cogroup(nodeIdsToWayIds)
        /* The `.distinct` above ensures that `wayIds` here will contain unique values. */
        .flatMap { case (_, (ns, wayIds)) => ns.map(n => (n, wayIds)) }
        .cache

    /* Not /that/ much duplication, as most Nodes are only needed by one Way (if any).
     * Any standalone Node whose `wayIds` is empty will be crushed away by the flatMap,
     * leaving this RDD as only those Nodes who were ever referenced by something.
     */
    val wayIdToNodes: RDD[(Long, Node)] =
      nodesToWayIds.flatMap { case (node, wayIds) => wayIds.map(wayId => (wayId, node)) }

    val edges: RDD[(Long, (Iterable[Way], Iterable[Node]))] = ways.cogroup(wayIdToNodes)

    val points: RDD[(Long, Node)] =
      nodesToWayIds.filter { case (_, wayIds) => wayIds.isEmpty }.map { case (n, _) => (n.meta.id, n) }

    (points, edges)
  }

  /** Given a likely unordered collection of Way versions and its associated Nodes,
    * construct GeoTrellis Lines and Polygons such that:
    *   - there is a Line present for every Way update
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
      case w :: rest if w.nodes.length < 4 && w.isClosed => work(rest, ls, ps)
      case w :: rest if w.nodes.length < 2 => work(rest, ls, ps)
      case w :: rest => {

        /* Y2K bug here, except it won't manifest until the end of the year 1 billion AD */
        val nextTime: Instant = rest.headOption.map(_.meta.timestamp).getOrElse(Instant.MAX)

        /* Each full set of Nodes that would have existed for each time slice. */
        val allSlices: List[(ElementMeta, Map[Long, Node])] =
          changedNodes(w.meta.timestamp, nextTime, nodes)
            .groupBy(_.meta.timestamp)
            .toList
            .sortBy { case (i, _) => i }
            .scanLeft((w.meta, recentNodes(w, nodes))) { case ((prevMeta, p), (_, changes)) =>

              /* All Nodes changed during this timeslice are assumed to have been
               * changed by the same user. Anything else would be highly unlikely.
               * Therefore, we can ask for the metadata of the first changed Node
               * we find (the `.head`).
               */
              val meta: ElementMeta = changes.head.meta

              /* The user who changed the Nodes in this timeslice is credited
               * with the creation of the `Line`, even if they weren't the one
               * who created the Way to begin with (unless the Node previously
               * existed, in which case the Way's author should be credited).
               */
              val credited : ElementMeta = if (meta.timestamp.isAfter(prevMeta.timestamp)) {
                prevMeta.copy(
                  user      = meta.user,
                  uid       = meta.uid,
                  changeset = meta.changeset,
                  timestamp = meta.timestamp,
                  minorVersion = prevMeta.minorVersion + 1
                )
              } else {
                prevMeta
              }

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
